"""
Streaming silver: bronze.sensor_reading (an Iceberg table read as a stream) to silver.sensor_window,
as a long-running CDE Spark Structured Streaming job (rsingh-ogx-stream-agg).

  normalise  bar -> psi (x 14.5038, config/streaming.json units); the original unit is kept in
             the row count by unit, so the mid-stream unit change on PI-105A stays visible
  windows    1-minute and 15-minute tumbling windows per tag on event time, with a 10-minute
             watermark: a reading up to 10 minutes late updates its window; later than that it
             is dropped and counted by the stream recon as LATE
  measures   count, min, max, mean, standard deviation, slope (least squares, per hour), late
             count (sent > 60 s after its event time), readings in bar, quality BAD / UNCERTAIN
  flags      flat (zero variance with enough readings), stuck (a flat 15-minute window), breach
             (max above the measurement's high threshold)
  write      foreachBatch in update mode: each batch's changed windows are MERGEd into
             silver.sensor_window on (sensor_tag, window_size, window_start), so a late event
             corrects its window instead of duplicating it

  spark-submit stream_telemetry_agg.py [--checkpoints URI] [--trigger 60s|available-now]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STREAM = C.load_json("config/streaming.json")
SPARK_CFG = STREAM["spark"]
QUERY = "stream-agg"
KIND = {"PI": "pressure_psi", "TI": "temperature_c", "VI": "vibration_mm_s", "CP": "wall_loss_mm_y"}
WINDOW_SCHEMA = (
    "sensor_tag string, tag_prefix string, measurement string, window_size string, window_start timestamp, "
    "window_end timestamp, n bigint, min_value double, max_value double, mean_value double, std_value double, "
    "slope_per_h double, n_late bigint, n_bar bigint, n_bad bigint, n_uncertain bigint, first_event_ts timestamp, "
    "last_event_ts timestamp, is_flat boolean, is_stuck boolean, is_breach boolean, high_threshold double, "
    "window_date date, _updated_at timestamp, _batch string")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    p.add_argument("--checkpoints", default=C.sibling(C.DEFAULT_LANDING, "checkpoints"))
    p.add_argument("--trigger", default=f"{SPARK_CFG['trigger_seconds']} seconds")
    p.add_argument("--await-seconds", type=int, default=0)
    p.add_argument("--source", default=None, help="ignored (the source is the bronze table)")
    p.add_argument("--input", default=None, help="ignored")
    return p


def normalised(df: DataFrame) -> DataFrame:
    bar = F.col("unit") == "bar"
    prefix = F.regexp_extract("sensor_tag", r"^([A-Z]+)-", 1)
    kind = F.create_map(*[x for k, v in KIND.items() for x in (F.lit(k), F.lit(v))])[prefix]
    return df.select(
        "sensor_tag", prefix.alias("tag_prefix"), kind.alias("measurement"), "event_ts", "quality",
        F.when(bar, F.col("value") * STREAM["units"]["bar_to_psi"]).otherwise(F.col("value")).alias("value"),
        bar.alias("was_bar"), (F.col("lateness_s") > 60).alias("is_late"),
        (F.unix_millis("event_ts") % 86_400_000 / 1000.0).alias("t"))


def _seconds(size: str) -> int:
    n, unit = size.split()
    return int(n) * {"minute": 60, "minutes": 60, "hour": 3600, "hours": 3600}[unit]


def with_windows(df: DataFrame, sizes: list[str]) -> DataFrame:
    """Each reading once per window size, with its window's end: one aggregation serves both sizes
    (a streaming query takes one stateful aggregation). The watermark goes on the window end, so a
    window's state is kept until the watermark passes its end."""
    sizes_col = F.explode(F.array(*[F.struct(F.lit(s).alias("size"), F.lit(_seconds(s)).alias("secs"))
                                     for s in sizes]))
    x = df.withColumn("_ws", sizes_col)
    start_s = F.floor(F.unix_seconds("event_ts") / F.col("_ws.secs")) * F.col("_ws.secs")
    return (x.withColumn("window_size", F.col("_ws.size"))
            .withColumn("w_end", F.timestamp_seconds(start_s + F.col("_ws.secs")))
            .withColumn("w_start", F.timestamp_seconds(start_s)).drop("_ws"))


def windowed(df: DataFrame) -> DataFrame:
    n = F.count("*")
    st, stt, sv, stv = F.sum("t"), F.sum(F.col("t") * F.col("t")), F.sum("value"), F.sum(F.col("t") * F.col("value"))
    denom = n * stt - st * st
    return (df.groupBy("sensor_tag", "tag_prefix", "measurement", "window_size", "w_end")
            .agg(n.alias("n"), F.min("value").alias("min_value"), F.max("value").alias("max_value"),
                 F.avg("value").alias("mean_value"), F.stddev_pop("value").alias("std_value"),
                 F.when(denom > 0, (n * stv - st * sv) / denom * 3600.0).alias("slope_per_h"),
                 F.sum(F.col("is_late").cast("int")).cast("bigint").alias("n_late"),
                 F.sum(F.col("was_bar").cast("int")).cast("bigint").alias("n_bar"),
                 F.sum((F.col("quality") == "BAD").cast("int")).cast("bigint").alias("n_bad"),
                 F.sum((F.col("quality") == "UNCERTAIN").cast("int")).cast("bigint").alias("n_uncertain"),
                 F.min("event_ts").alias("first_event_ts"), F.max("event_ts").alias("last_event_ts"),
                 F.min("w_start").alias("w_start")))


def finish(df: DataFrame, batch_key: str) -> DataFrame:
    th = STREAM["thresholds"]
    high = F.create_map(*[x for k, v in th.items() for x in (F.lit(k), F.lit(float(v["high"])))])[F.col("measurement")]
    mins = SPARK_CFG["min_readings_per_window"]
    enough = F.col("n") >= F.create_map(*[x for k, v in mins.items() for x in (F.lit(k), F.lit(v))])[F.col("window_size")]
    flat = enough & (F.coalesce(F.col("std_value"), F.lit(1.0)) < 1e-9)
    return df.select(
        "sensor_tag", "tag_prefix", "measurement", "window_size", F.col("w_start").alias("window_start"),
        F.col("w_end").alias("window_end"), "n", "min_value", "max_value", "mean_value", "std_value", "slope_per_h",
        "n_late", "n_bar", "n_bad", "n_uncertain", "first_event_ts", "last_event_ts", flat.alias("is_flat"),
        (flat & (F.col("window_size") == "15 minutes")).alias("is_stuck"),
        (F.col("max_value") > high).alias("is_breach"), high.alias("high_threshold"),
        F.to_date(F.col("w_start")).alias("window_date"), F.current_timestamp().alias("_updated_at"),
        F.lit(batch_key).alias("_batch"))


def merge_batch(names: C.Names):
    target = names.t("silver", "sensor_window")

    def batch(df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        out = finish(df, f"{QUERY}:{batch_id}")
        C.ensure_table(spark, target, WINDOW_SCHEMA, ["window_date"])
        out.createOrReplaceTempView("ogx_window_updates")
        spark.sql(f"""
            MERGE INTO {target} t USING ogx_window_updates s
            ON t.sensor_tag = s.sensor_tag AND t.window_size = s.window_size AND t.window_start = s.window_start
               AND t.window_date = s.window_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        print(f"batch {batch_id}: windows merged into {target}", flush=True)

    return batch


def run(spark, argv=None):
    args, _ = parser().parse_known_args(argv)
    C.configure(spark)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    source = names.t("bronze", "sensor_reading")
    readings = (spark.readStream.format("iceberg").option("streaming-skip-delete-snapshots", "true")
                .option("streaming-skip-overwrite-snapshots", "true").load(source))
    base = with_windows(normalised(readings), SPARK_CFG["windows"]).withWatermark("w_end", SPARK_CFG["watermark"])
    windows = windowed(base)
    w = (windows.writeStream.queryName(QUERY).outputMode("update").foreachBatch(merge_batch(names))
         .option("checkpointLocation", f"{C.as_uri(args.checkpoints)}/agg"))
    w = w.trigger(availableNow=True) if args.trigger == "available-now" else w.trigger(processingTime=args.trigger)
    q = w.start()
    print(f"{QUERY}: started (from {source}, watermark {SPARK_CFG['watermark']}, windows "
          f"{', '.join(SPARK_CFG['windows'])}, trigger {args.trigger})", flush=True)
    if args.await_seconds:
        q.awaitTermination(args.await_seconds)
        q.stop()
    else:
        q.awaitTermination()
    return q


def main() -> int:
    spark = C.get_spark("ogx-stream-agg")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
