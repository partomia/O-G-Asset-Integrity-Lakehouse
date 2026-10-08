"""
The sensor producer as a CDE Spark job (rsingh-ogx-stream-produce): the same seeded generator,
modes and control messages as stream/producer/sensor_producer.py (the CAI job ogx-stream-producer),
writing to Kafka through Spark's Kafka sink instead of kafka-python. CDE reaches the brokers on
9093 where a laptop cannot, and needs no Python packages beyond PySpark.

  --setup      create the ogx.* topics if absent (Kafka AdminClient over py4j: partitions from
               config/streaming.json, the brokers' default replication)
  --mode backfill --start-date 2026-10-04 --days 5 [--interval 30]
  --mode live --duration 0 (until stopped) [--interval 5]

Kafka settings as for stream_telemetry_bronze.py (spark.ogx.kafka.*). Messages are buffered on the
driver and written in batches of --batch records; a live run flushes every tick.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402

C._import_path()
from stream.producer import sensor_producer as SP  # noqa: E402
from stream_telemetry_bronze import STREAM, kafka_options  # noqa: E402


class SparkKafkaSink:
    def __init__(self, spark, opts: dict, batch: int):
        self.spark, self.opts, self.batch = spark, opts, batch
        self.buf: list[tuple[str, str, str]] = []
        self.sent = 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        self.buf.append((topic, msg.get("sensor_tag") or msg.get("topic"), json.dumps(msg, separators=(",", ":"))))
        self.sent += 1
        if len(self.buf) >= self.batch:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        df = self.spark.createDataFrame(self.buf, "topic string, key string, value string")
        df.write.format("kafka").options(**self.opts).save()
        self.buf = []

    def close(self) -> None:
        self.flush()


class LiveSink(SparkKafkaSink):
    """Live mode: flush on every tick, so readings leave within the 5-second interval."""

    def send(self, topic, msg, send_ts):
        super().send(topic, msg, send_ts)
        now = time.time()
        if now - getattr(self, "_last", 0) >= 1:
            self.flush()
            self._last = now


def create_topics(spark, opts: dict) -> dict:
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    for k, v in opts.items():
        props.put(k[len("kafka."):], v)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    try:
        existing = set(admin.listTopics().names().get())
        out, new = {}, []
        for t in STREAM["topics"].values():
            if t["name"] in existing:
                out[t["name"]] = "EXISTS"
                continue
            nt = jvm.org.apache.kafka.clients.admin.NewTopic(
                t["name"], jvm.java.util.Optional.of(t["partitions"]), jvm.java.util.Optional.empty())
            cfg = jvm.java.util.HashMap()
            cfg.put("retention.ms", str(t["retention_hours"] * 3600_000))
            new.append(nt.configs(cfg))
            out[t["name"]] = "CREATED"
        if new:
            lst = jvm.java.util.ArrayList()
            for nt in new:
                lst.add(nt)
            admin.createTopics(lst).all().get()
        return out
    finally:
        admin.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--setup", action="store_true")
    ap.add_argument("--mode", choices=["backfill", "live", "none"], default="backfill")
    ap.add_argument("--start-date", default=C.CFG["business_dates"][0])
    ap.add_argument("--days", type=int, default=len(C.CFG["business_dates"]))
    ap.add_argument("--interval", type=int, default=None)
    ap.add_argument("--duration", type=int, default=0)
    ap.add_argument("--batch", type=int, default=250_000)
    args, _ = ap.parse_known_args(argv if argv is not None else sys.argv[1:])
    spark = C.get_spark("ogx-stream-produce")
    opts = kafka_options(spark)
    print(f"brokers {opts['kafka.bootstrap.servers']} ({opts['kafka.security.protocol']})", flush=True)
    if args.setup:
        print("topics:", create_topics(spark, opts), flush=True)
    if args.mode == "none":
        return 0
    run_id = f"{args.mode}-{uuid.uuid4().hex[:8]}"
    t0 = time.time()
    if args.mode == "backfill":
        sink = SparkKafkaSink(spark, opts, args.batch)
        interval = args.interval or 30
        start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
        print(f"backfill {args.start_date} + {args.days} days at {interval} s, run {run_id}", flush=True)
        SP.backfill(sink, start, args.days, interval, run_id)
    else:
        sink = LiveSink(spark, opts, args.batch)
        interval = args.interval or SP.P["interval_s"]
        print(f"live at {interval} s per tag, run {run_id}", flush=True)
        SP.live(sink, args.duration, interval, run_id)
    sink.close()
    print(f"sent {sink.sent:,} messages in {time.time() - t0:.0f} s (run {run_id})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
