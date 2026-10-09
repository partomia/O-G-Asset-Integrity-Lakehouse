"""
Stage 5 - Gold: the dimensional model for one business date, keyed on the asset master's asset_id.

  dim_date              one row per calendar day of the business dates
  dim_asset             SCD2 over asset.golden_asset: a new version when a tracked attribute changes
                        (criticality, status, design pressure, inspection interval, last inspection)
  fact_inspection       silver.inspection_finding, resolved to asset_id through asset_xref
  fact_document         one row per catalogued object per asset (inspection PDFs, drone video, drawings,
                        well logs), with the raw path the app opens
  fact_sensor_daily     silver.sensor_window 15-minute windows per sensor per day: mean, max, slope,
                        stuck and breach windows, late readings; resolved to asset_id
  fact_work_order       silver.erp_work_order with asset_id
  fact_asset_risk_daily one row per active asset per day: latest wall loss, its trend, sensor anomaly
                        counts, corrective work orders in 30 days, days overdue for inspection,
                        criticality, and a transparent rule score (0-100) that the equipment_risk model
                        is measured against

Each fact replaces the date's partition; dim_asset is rebuilt from the golden history (idempotent).

  spark-submit build_gold.py --business-date 2026-10-06 [--db-prefix P]
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "gold"
TRACKED = ["criticality", "status", "design_pressure_psi", "inspection_interval_days", "last_inspection_date", "is_active"]
CRIT_WEIGHT = {"A": 1.0, "B": 0.6, "C": 0.3}
THRESHOLD_OVERRIDES = C.load_json("config/streaming.json").get("threshold_overrides", {"-": 0.0})
STUCK_MIN_READINGS = C.load_json("config/streaming.json")["spark"]["min_readings_per_window"]["15 minutes"]


def parser():
    return C.base_parser(__doc__)


def day_lit(d):
    return F.lit(d.isoformat()).cast("date")


def xref_for(spark, names, d, systems):
    x = spark.table(names.t("asset", "asset_xref")).where(F.col("as_of_date") == day_lit(d))
    return x.where(F.col("src_system").isin(systems))


def pairs_for(spark, names, d, systems):
    """(src_key -> asset_id) for keyed candidates (documents): from match_pair, decision MATCHED."""
    p = spark.table(names.t("asset", "match_pair")).where((F.col("as_of_date") == day_lit(d)) & (F.col("decision") == "MATCHED"))
    return p.where(F.col("src_system").isin(systems)).select("src_key", "asset_id", "rule").dropDuplicates(["src_key"])


def dim_asset(spark, names, d):
    g = spark.table(names.t("asset", "golden_asset")).where(F.col("as_of_date") <= day_lit(d))
    w = Window.partitionBy("asset_id").orderBy("as_of_date")
    sig = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in TRACKED]), 256)
    v = (g.withColumn("_sig", sig).withColumn("_prev", F.lag("_sig").over(w))
         .where(F.col("_prev").isNull() | (F.col("_prev") != F.col("_sig")))
         .withColumn("valid_from", F.col("as_of_date"))
         .withColumn("valid_to", F.lead("as_of_date").over(w))
         .withColumn("version", F.row_number().over(w)))
    v = (v.withColumn("is_current", F.col("valid_to").isNull())
         .withColumn("asset_sk", F.concat_ws("#", "asset_id", F.col("version").cast("string")))
         .drop("_sig", "_prev", "as_of_date"))
    C.replace_table(v, names.t("gold", "dim_asset"))
    return v.count()


def dim_date(spark, names):
    days = sorted(set(C.CFG["business_dates"] + [C.CFG["live_date"]]))
    df = spark.createDataFrame([(x,) for x in days], "d string").select(
        F.col("d").cast("date").alias("date"), F.date_format(F.col("d").cast("date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.dayofweek(F.col("d").cast("date")).alias("day_of_week"), F.weekofyear(F.col("d").cast("date")).alias("week"),
        F.month(F.col("d").cast("date")).alias("month"), F.year(F.col("d").cast("date")).alias("year"),
        F.col("d").isin(C.CFG["business_dates"]).alias("is_business_date"))
    C.replace_table(df, names.t("gold", "dim_date"))


def write_fact(spark, names, name, df, d) -> int:
    target = names.t("gold", name)
    df = df.withColumn("business_date", day_lit(d))
    n = df.count()
    if n == 0:
        if C.table_exists(spark, target):
            spark.sql(f"DELETE FROM {target} WHERE business_date = DATE '{d.isoformat()}'")
        return 0
    C.write_partitions(df, target, ["business_date"])
    return n


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    d = args.business_date
    C.require_completed(spark, names, "asset", d)
    audit = C.Audit(spark, names, "build_gold", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED")
    t = names.t
    out = {}
    try:
        dim_date(spark, names)
        out["dim_asset"] = dim_asset(spark, names, d)
        golden = spark.table(t("asset", "golden_asset")).where(F.col("as_of_date") == day_lit(d))

        # inspections
        insp_map = pairs_for(spark, names, d, ["inspection"])
        f = spark.table(t("silver", "inspection_finding")).where(F.col("business_date") == day_lit(d))
        fi = (f.join(insp_map, f.doc_id == insp_map.src_key, "left").drop("src_key", "business_date")
              .withColumn("inspection_id", F.col("report_no"))
              .withColumn("inspected_at", F.to_timestamp("inspection_ts", "yyyy-MM-dd HH:mm"))
              .withColumn("asset_resolved", F.col("asset_id").isNotNull()))
        out["fact_inspection"] = write_fact(spark, names, "fact_inspection", fi, d)

        # documents
        doc_map = pairs_for(spark, names, d, ["inspection_file", "welllogs_file", "drone", "inspection"])
        objs = spark.table(t("bronze", "doc_object")).where(
            (F.col("_business_date") == day_lit(d)) & (F.col("ingest_status") != "QUARANTINED"))
        fd = (objs.join(doc_map, objs.doc_id == doc_map.src_key, "left")
              .select("doc_id", "asset_id", "source", "format", "file_name", "mime_type", "size_bytes", "raw_path",
                      "captured_at", "ingest_status", "asset_hint", F.col("rule").alias("resolved_by")))
        dt = spark.table(t("silver", "drawing_tag")).where("format = 'pdf'") if C.table_exists(spark, t("silver", "drawing_tag")) else None
        if dt is not None:
            tag_map = xref_for(spark, names, d, ["drawing"]).select(F.col("src_name").alias("tag"), "asset_id").dropDuplicates(["tag"])
            # tags are read from the vector PDF; its raster PNG (same sheet stem) links to the same assets
            stem = F.regexp_extract(F.col("file_name"), r"^(.*)\.[^.]+$", 1)
            drawings = objs.where("source = 'drawings'").withColumn("_stem", stem)
            sheets = (dt.where(F.col("business_date") == day_lit(d)).join(tag_map, "tag", "inner")
                      .select("doc_id", "asset_id").distinct()
                      .join(drawings.select("doc_id", "_stem"), "doc_id", "inner").select("_stem", "asset_id").distinct())
            drw = drawings.join(sheets, "_stem", "inner").select(
                "doc_id", "asset_id", "source", "format", "file_name", "mime_type", "size_bytes", "raw_path",
                "captured_at", "ingest_status", "asset_hint", F.lit("DRAWING_TAG").alias("resolved_by"))
            fd = fd.where("source <> 'drawings'").unionByName(drw)
        out["fact_document"] = write_fact(spark, names, "fact_document", fd, d)

        # sensors
        smap = xref_for(spark, names, d, ["sensor"]).select(F.col("src_name").alias("sensor_tag"), "asset_id").dropDuplicates(["sensor_tag"])
        sw = spark.table(t("silver", "sensor_window")).where(
            (F.col("window_date") == day_lit(d)) & (F.col("window_size") == "15 minutes")) \
            if C.table_exists(spark, t("silver", "sensor_window")) else None
        if sw is not None:
            fs = (sw.groupBy("sensor_tag", "measurement")
                  .agg(F.sum("n").alias("readings"), F.avg("mean_value").alias("mean_value"),
                       F.max("max_value").alias("max_value"), F.avg("slope_per_h").alias("mean_slope_per_h"),
                       F.sum((F.col("is_stuck") | ((F.col("std_value") < 1e-9) & (F.col("n") >= STUCK_MIN_READINGS)))
                             .cast("int")).alias("stuck_windows"),
                       F.sum((F.col("max_value") > F.coalesce(F.create_map(*[x for k, v in THRESHOLD_OVERRIDES.items() for x in (F.lit(k), F.lit(float(v)))])[F.substring("sensor_tag", 1, 4)],
                                                             F.col("high_threshold"))).cast("int")).alias("breach_windows"),
                       F.sum("n_late").alias("late_readings"), F.sum("n_bar").alias("bar_readings"),
                       F.count("*").alias("windows"))
                  .join(smap, "sensor_tag", "left"))
            out["fact_sensor_daily"] = write_fact(spark, names, "fact_sensor_daily", fs, d)

        # work orders
        wo = spark.table(t("silver", "erp_work_order")).where(F.col("business_date") == day_lit(d))
        fw = wo.drop("business_date").withColumn("asset_id", F.concat(F.lit("OGX-"), F.col("equnr")))
        out["fact_work_order"] = write_fact(spark, names, "fact_work_order", fw, d)

        # risk
        out["fact_asset_risk_daily"] = write_fact(spark, names, "fact_asset_risk_daily", risk_frame(spark, names, d, golden), d)

        load_kpis(spark, names, d)

        for k, v in out.items():
            audit.load(STAGE, k, "COMMITTED", rows_out=v)
            audit.transform(f"build {k}", "historise" if k == "dim_asset" else "enrich", "silver.*,asset.*", t("gold", k), 0, v, "")
        audit.load(STAGE, "*", "COMPLETED", rows_out=sum(out.values()))
        print(f"gold {d}: " + ", ".join(f"{k} {v}" for k, v in out.items()), flush=True)
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return out


def load_kpis(spark, names, d) -> None:
    """config/kpi.json -> ref.kpi_definition and ref.kpi_parameter for the business date, so the
    semantic views read a parameter from one place and a change is dated."""
    cfg = C.load_json("config/kpi.json")
    defs = [(k["kpi_code"], k["name"], k["definition"], k["formula"], k["grain"], k["certified_view"], k["owner"],
             k["version"], k["certified_on"], k["glossary_term"], d) for k in cfg["kpis"]]
    C.write_partitions(C.rows_df(spark, defs, "kpi_code string, name string, definition string, formula string, "
                                 "grain string, certified_view string, owner string, version string, "
                                 "certified_on string, glossary_term string, business_date date"),
                       names.t("ref", "kpi_definition"), ["business_date"])
    params = []
    for name, v in cfg["parameters"].items():
        if isinstance(v, dict):
            params += [(f"{name}.{k}", float(x), None, d) for k, x in v.items()]
        elif isinstance(v, list):
            params.append((name, None, ",".join(v), d))
        else:
            params.append((name, float(v), None, d))
    C.write_partitions(C.rows_df(spark, params, "parameter string, value double, text_value string, business_date date"),
                       names.t("ref", "kpi_parameter"), ["business_date"])


def risk_frame(spark, names, d, golden):
    t = names.t
    upto = F.col("business_date") <= day_lit(d)
    fi = spark.table(t("gold", "fact_inspection")).where(upto & F.col("asset_id").isNotNull())
    w = Window.partitionBy("asset_id").orderBy(F.desc("business_date"), F.desc("inspected_at"))
    insp = (fi.withColumn("_rn", F.row_number().over(w))
            .groupBy("asset_id").agg(
                F.max(F.when(F.col("_rn") == 1, F.col("wall_loss_pct"))).alias("wall_loss_pct"),
                F.max(F.when(F.col("_rn") == 2, F.col("wall_loss_pct"))).alias("prev_wall_loss_pct"),
                F.max(F.when(F.col("_rn") == 1, F.col("coating"))).alias("coating"),
                F.max(F.when(F.col("_rn") == 1, F.col("cui"))).alias("cui"),
                F.max("business_date").alias("last_report_date")))
    sens = None
    if C.table_exists(spark, t("gold", "fact_sensor_daily")):
        sens = (spark.table(t("gold", "fact_sensor_daily"))
                .where((F.col("business_date") <= day_lit(d)) & (F.col("business_date") > day_lit(d - timedelta(days=3))))
                .groupBy("asset_id").agg(F.sum("breach_windows").alias("breach_windows_3d"),
                                         F.sum("stuck_windows").alias("stuck_windows_3d"),
                                         F.max(F.when(F.col("measurement") == "vibration_mm_s", F.col("max_value"))).alias("max_vibration_mm_s"),
                                         F.max(F.when(F.col("measurement") == "wall_loss_mm_y", F.col("mean_value"))).alias("wall_loss_rate_mm_y")))
    wo = (spark.table(t("gold", "fact_work_order"))
          .where(upto & (F.col("business_date") > day_lit(d - timedelta(days=30))) & F.col("is_corrective"))
          .groupBy("asset_id").agg(F.count("*").alias("corrective_wo_30d")))
    g = golden.where("is_active").select("asset_id", "tag", "asset_class", "facility_id", "criticality",
                                         "last_inspection_date", "inspection_interval_days")
    r = g.join(insp, "asset_id", "left").join(wo, "asset_id", "left")
    r = r.join(sens, "asset_id", "left") if sens is not None else r.withColumn("breach_windows_3d", F.lit(None).cast("long")) \
        .withColumn("stuck_windows_3d", F.lit(None).cast("long")).withColumn("max_vibration_mm_s", F.lit(None).cast("double")) \
        .withColumn("wall_loss_rate_mm_y", F.lit(None).cast("double"))
    overdue = F.greatest(F.lit(0), F.datediff(day_lit(d), F.date_add("last_inspection_date", F.col("inspection_interval_days"))))
    crit = F.create_map(*[x for k, v in CRIT_WEIGHT.items() for x in (F.lit(k), F.lit(v))])[F.col("criticality")]
    wl = F.coalesce(F.col("wall_loss_pct"), F.lit(0.0))
    trend = F.greatest(F.lit(0.0), wl - F.coalesce(F.col("prev_wall_loss_pct"), wl))
    score = (F.least(F.lit(35.0), wl * 1.2) + F.least(F.lit(10.0), trend * 2)
             + F.least(F.lit(20.0), F.coalesce(F.col("breach_windows_3d"), F.lit(0)) * 0.5)
             + F.least(F.lit(10.0), F.coalesce(F.col("corrective_wo_30d"), F.lit(0)) * 3.0)
             + F.least(F.lit(10.0), overdue / 30.0)
             + F.when(F.col("cui") == "YES", 5.0).otherwise(0.0)
             + F.when(F.col("coating") == "POOR", 5.0).otherwise(0.0)) * (0.6 + 0.4 * F.coalesce(crit, F.lit(0.3)))
    return (r.withColumn("days_overdue", overdue).withColumn("wall_loss_trend_pct", trend)
            .withColumn("rule_score", F.round(F.least(F.lit(100.0), score), 1))
            .withColumn("risk_band", F.when(F.col("rule_score") >= 40, "HIGH").when(F.col("rule_score") >= 20, "MEDIUM").otherwise("LOW")))


def main() -> int:
    spark = C.get_spark("ogx-gold")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
