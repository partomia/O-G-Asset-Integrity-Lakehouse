"""
Streaming bronze: Kafka topics ogx.sensor.telemetry, ogx.scada.alarm and ogx.control to Iceberg,
as a long-running CDE Spark Structured Streaming job (rsingh-ogx-stream-bronze).

  source     Kafka (SASL_SSL, PLAIN, the workload user; startingOffsets on the first run, then
             the checkpoint), or --source files: the producer's JSON-lines records (same
             key / timestamp / value shape) for laptops and CI
  validate   each message against contracts/stream/<topic>.json (required, type, pattern,
             allowed values); bad messages -> bronze.stream_quarantine with reason codes
  write      bronze.sensor_reading and bronze.scada_alarm (partitioned by event_date, event_hour),
             bronze.stream_control (the producer's per-minute counts), with Kafka topic,
             partition, offset and send time kept for lineage and lateness
  exactly    foreachBatch; each table's commit carries snapshot property ogx.stream-batch =
  once       <query id>:<batch id> (the query id lives in the checkpoint), and a replayed batch
             (restart after a crash between commits) skips the tables that already hold it: no
             gap, no duplicate

  spark-submit stream_telemetry_bronze.py [--source kafka|files] [--input DIR] [--checkpoints URI]
      [--trigger 60s|available-now] [--starting-offsets earliest|latest] [--await-seconds N]
Kafka settings come from Spark conf spark.ogx.kafka.* (bootstrap, user, password, truststore),
else the OGX_KAFKA_* environment; the password is never printed (Spark redacts *password* confs).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STREAM = C.load_json("config/streaming.json")
TOPICS = {k: v["name"] for k, v in STREAM["topics"].items()}
QUERY = "stream-bronze"
QUARANTINE_SCHEMA = ("kafka_topic string, kafka_partition int, kafka_offset bigint, kafka_ts timestamp, record_key string, "
                     "value string, reject_reasons array<string>, event_date date, _ingested_at timestamp")
KAFKA_COLS = ["kafka_topic", "kafka_partition", "kafka_offset", "kafka_ts"]


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    p.add_argument("--source", default="kafka", choices=["kafka", "files"])
    p.add_argument("--input", default=None, help="--source files: the producer's --out folder")
    p.add_argument("--checkpoints", default=C.sibling(C.DEFAULT_LANDING, "checkpoints"))
    p.add_argument("--trigger", default=f"{STREAM['spark']['trigger_seconds']}s")
    p.add_argument("--starting-offsets", default=STREAM["spark"]["starting_offsets"])
    p.add_argument("--max-offsets", type=int, default=STREAM["spark"]["max_offsets_per_trigger"])
    p.add_argument("--await-seconds", type=int, default=0, help="stop after N seconds (0: run until stopped)")
    return p


# ---------------------------------------------------------------- source


def kafka_options(spark) -> dict:
    def conf(key, env):
        return spark.conf.get(f"spark.ogx.kafka.{key}", None) or os.environ.get(env)

    bootstrap = conf("bootstrap", "OGX_KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise SystemExit("Kafka bootstrap servers: set spark.ogx.kafka.bootstrap or OGX_KAFKA_BOOTSTRAP")
    proto = conf("protocol", "OGX_KAFKA_SECURITY_PROTOCOL") or "SASL_SSL"
    opts = {"kafka.bootstrap.servers": bootstrap, "kafka.security.protocol": proto}
    if proto.startswith("SASL"):
        user, password = conf("user", "OGX_KAFKA_USER"), conf("password", "OGX_KAFKA_PASSWORD")
        opts["kafka.sasl.mechanism"] = conf("mechanism", "OGX_KAFKA_SASL_MECHANISM") or "PLAIN"
        opts["kafka.sasl.jaas.config"] = ("org.apache.kafka.common.security.plain.PlainLoginModule required "
                                          f'username="{user}" password="{password}";')
    truststore = conf("truststore", "OGX_KAFKA_TRUSTSTORE")
    if "SSL" in proto and truststore:
        opts["kafka.ssl.truststore.location"] = truststore
        opts["kafka.ssl.truststore.type"] = "PEM" if truststore.endswith(".pem") else "JKS"
    return opts


def read_source(spark, args) -> DataFrame:
    """kafka_topic, kafka_partition, kafka_offset, kafka_ts, record_key, record_value: one row per record."""
    if args.source == "files":
        frames = []
        for topic in TOPICS.values():
            raw = (spark.readStream.schema("key string, timestamp bigint, value string")
                   .option("maxFilesPerTrigger", 24).json(f"{C.as_uri(args.input)}/{topic}"))
            frames.append(raw.select(F.lit(topic).alias("kafka_topic"), F.lit(0).alias("kafka_partition"),
                                     F.lit(None).cast("bigint").alias("kafka_offset"),
                                     F.timestamp_millis("timestamp").alias("kafka_ts"),
                                     F.col("key").alias("record_key"), F.col("value").alias("record_value")))
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        return out
    raw = (spark.readStream.format("kafka").options(**kafka_options(spark))
           .option("subscribe", ",".join(TOPICS.values())).option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets).option("failOnDataLoss", "false").load())
    return raw.select(F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                      F.col("timestamp").alias("kafka_ts"), F.col("key").cast("string").alias("record_key"),
                      F.col("value").cast("string").alias("record_value"))


# ---------------------------------------------------------------- validate


SPARK_TYPE = {"string": "string", "double": "double", "bigint": "bigint", "int": "int", "timestamp": "string"}


def parse_topic(df: DataFrame, contract: dict) -> DataFrame:
    """The contract's fields from the JSON value, typed, with reject reasons."""
    fields = contract["fields"]
    schema = ", ".join(f"`{f['name']}` string" for f in fields)
    j = F.from_json("record_value", schema)
    rejects = [F.when(j.isNull(), F.lit("value:MALFORMED_JSON"))]
    cols = []
    for f in fields:
        name, raw = f["name"], j[f["name"]]
        present = raw.isNotNull() & (F.trim(raw) != "")
        if f.get("required"):
            rejects.append(F.when(~present, F.lit(f"{name}:REQUIRED")))
        if f["type"] == "timestamp":
            typed = F.to_timestamp(raw)
            rejects.append(F.when(present & typed.isNull(), F.lit(f"{name}:INVALID_TIMESTAMP")))
        elif f["type"] in ("double", "bigint", "int"):
            typed = raw.cast(SPARK_TYPE[f["type"]])
            rejects.append(F.when(present & typed.isNull(), F.lit(f"{name}:NOT_NUMBER")))
        else:
            typed = raw
        if "pattern" in f:
            rejects.append(F.when(present & ~raw.rlike(f["pattern"]), F.lit(f"{name}:PATTERN")))
        if "allowed" in f:
            rejects.append(F.when(present & ~raw.isin(f["allowed"]), F.lit(f"{name}:NOT_ALLOWED")))
        cols.append(typed.alias(name))
    reasons = F.filter(F.array(*rejects), lambda x: x.isNotNull())
    extra = [] if any(f["name"] == "producer_run" for f in fields) else \
        [F.get_json_object("record_value", "$.producer_run").alias("producer_run")]
    return df.select(*KAFKA_COLS, "record_key", "record_value", *cols, *extra, reasons.alias("reject_reasons"))


# ---------------------------------------------------------------- write


def committed(spark, table: str, key: str) -> bool:
    if not C.table_exists(spark, table):
        return False
    rows = spark.sql(f"SELECT summary['ogx.stream-batch'] AS b FROM {table}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 50").collect()
    return any(r["b"] == key for r in rows)


def append_once(df: DataFrame, table: str, key: str, partition_cols=()) -> int:
    spark = df.sparkSession
    if committed(spark, table, key):
        print(f"{table}: batch {key} already committed, skipped (replay)", flush=True)
        return 0
    n = df.count()
    if n == 0:
        return 0
    if C.table_exists(spark, table):
        df.writeTo(table).option("snapshot-property.ogx.stream-batch", key).append()
    else:
        C._create(df, table, partition_cols).option("snapshot-property.ogx.stream-batch", key).create()
    return n


def with_time_parts(df: DataFrame) -> DataFrame:
    return (df.withColumn("event_date", F.to_date("event_ts"))
            .withColumn("event_hour", F.hour("event_ts"))
            .withColumn("lateness_s", (F.unix_millis("kafka_ts") - F.unix_millis("event_ts")) / 1000.0)
            .withColumn("_ingested_at", F.current_timestamp()))


def process(names: C.Names, contracts: dict, query: dict):
    reading_t, alarm_t = names.t("bronze", "sensor_reading"), names.t("bronze", "scada_alarm")
    control_t, quarantine_t = names.t("bronze", "stream_control"), names.t("bronze", "stream_quarantine")

    def batch(df: DataFrame, batch_id: int) -> None:
        while "id" not in query:   # set once start() returns
            time.sleep(0.1)
        key = f"{query['id']}:{batch_id}"
        df = df.persist()
        bad_all = None
        counts = {}
        for kind, topic in TOPICS.items():
            parsed = parse_topic(df.where(F.col("kafka_topic") == topic), contracts[topic])
            good = parsed.where(F.size("reject_reasons") == 0).drop("reject_reasons", "record_value", "record_key")
            bad = parsed.where(F.size("reject_reasons") > 0).select(
                *KAFKA_COLS, "record_key", "record_value", "reject_reasons",
                F.coalesce(F.to_date(F.get_json_object("record_value", "$.event_ts")), F.to_date("kafka_ts")).alias("event_date"),
                F.current_timestamp().alias("_ingested_at"))
            bad_all = bad if bad_all is None else bad_all.unionByName(bad)
            if kind == "control":
                good = (good.withColumn("event_date", F.to_date("minute"))
                        .withColumn("_ingested_at", F.current_timestamp()))
                counts[kind] = append_once(good, control_t, key, ["event_date"])
            else:
                target = reading_t if kind == "telemetry" else alarm_t
                counts[kind] = append_once(with_time_parts(good), target, key, ["event_date", "event_hour"])
        counts["quarantine"] = append_once(bad_all, quarantine_t, key, ["event_date"])
        df.unpersist()
        print(f"batch {batch_id}: " + ", ".join(f"{k} {v}" for k, v in counts.items()), flush=True)

    return batch


def run(spark, argv=None):
    args, _ = parser().parse_known_args(argv)
    C.configure(spark)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    contracts = {c["topic"]: c for c in C.stream_contracts().values()}
    source = read_source(spark, args)
    query = {}
    w = (source.writeStream.queryName(QUERY).foreachBatch(process(names, contracts, query))
         .option("checkpointLocation", f"{C.as_uri(args.checkpoints)}/bronze"))
    w = w.trigger(availableNow=True) if args.trigger == "available-now" else w.trigger(processingTime=args.trigger)
    q = w.start()
    query["id"] = str(q.id)
    print(f"{QUERY}: started (query {q.id}, {args.source}, trigger {args.trigger}, checkpoint {args.checkpoints}/bronze)", flush=True)
    if args.await_seconds:
        q.awaitTermination(args.await_seconds)
        q.stop()
    else:
        q.awaitTermination()
    return q


def main() -> int:
    spark = C.get_spark("ogx-stream-bronze")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
