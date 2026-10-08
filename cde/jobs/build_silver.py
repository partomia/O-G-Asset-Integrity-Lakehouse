"""
Stage 3 - Silver (records): typed and standardised ERP data for one business date. The extractor
outputs are written by extract_unstructured.py and the sensor windows by stream_telemetry_agg.py.

  erp_asset         the asset register as of the date: the first full load plus every CDC event up to
                    the date applied in changed_at order (I insert, U field update, D delete), typed
                    (dates in either ERP format, design pressure as a number), then MERGEd into
                    silver.erp_asset on equnr: an unchanged row is not touched, so the snapshots show
                    exactly what each day's CDC changed (time travel per asset)
  erp_asset_change  one row per field change applied (equnr, field, old, new, changed_at): the SCD2
                    source for gold.dim_asset
  erp_work_order    the date's work orders, typed, one row per aufnr (a re-sent order keeps the latest)

  spark-submit build_silver.py --business-date 2026-10-05 [--db-prefix P]
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "silver"
ASSET_COLS = [c["name"] for c in C.contracts()["erp_asset"]["columns"]]
ASSET_SCHEMA = (
    "equnr string, tplnr string, eqktx string, eqart string, swerk string, abckz string, werks_mat string, "
    "design_press_psi double, inbdt date, status string, gps_lat double, gps_lon double, chain_from_km double, "
    "chain_to_km double, insp_interval_d int, last_insp_date date, is_deleted boolean, last_changed_at timestamp, "
    "last_changed_by string, as_of_date date")
CHANGE_SCHEMA = ("equnr string, op string, field string, old_value string, new_value string, changed_at timestamp, "
                 "changed_by string, business_date date")


def parser():
    return C.base_parser(__doc__)


def _date(v):
    if v in (None, ""):
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            pass
    return None


def _num(v, kind=float):
    try:
        return kind(v) if v not in (None, "") else None
    except ValueError:
        return None


def typed_asset(rec: dict) -> dict:
    out = dict(rec)
    for k in ("inbdt", "last_insp_date"):
        out[k] = _date(rec.get(k))
    for k in ("design_press_psi", "gps_lat", "gps_lon", "chain_from_km", "chain_to_km"):
        out[k] = _num(rec.get(k))
    out["insp_interval_d"] = _num(rec.get("insp_interval_d"), int)
    return out


def asset_state(spark, names, d: date) -> tuple[dict, list]:
    """equnr -> register row as of d, and the field changes applied on d."""
    day = F.lit(d.isoformat()).cast("date")
    full = spark.table(names.t("bronze", "erp_asset"))
    first = full.agg(F.min("_business_date")).collect()[0][0]
    if first is None or first > d:
        return {}, []
    state = {r["equnr"]: {**{c: r[c] for c in ASSET_COLS}, "is_deleted": False, "last_changed_at": None,
                          "last_changed_by": "INITIAL_LOAD"}
             for r in full.where(F.col("_business_date") == F.lit(first)).collect()}
    changes = []
    cdc_t = names.t("bronze", "erp_asset_cdc")
    if C.table_exists(spark, cdc_t):
        events = (spark.table(cdc_t).where(F.col("_business_date") <= day)
                  .select("op", "equnr", "changed_at", "changed_by", "fields", "_business_date")
                  .orderBy("changed_at", "equnr").collect())
        for e in events:
            fields = json.loads(e["fields"]) if e["fields"] else {}
            ts = datetime.strptime(e["changed_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            row = state.get(e["equnr"])
            if e["op"] == "I" or row is None:
                row = {c: None for c in ASSET_COLS} | {"equnr": e["equnr"], "is_deleted": False}
                state[e["equnr"]] = row
            for k, v in fields.items():
                if k in ASSET_COLS:
                    if e["_business_date"] == d and str(row.get(k)) != str(v):
                        changes.append((e["equnr"], e["op"], k, None if row.get(k) is None else str(row.get(k)),
                                        None if v is None else str(v), ts, e["changed_by"], d))
                    row[k] = None if v is None else str(v)
            if e["op"] == "D":
                row["is_deleted"] = True
                if e["_business_date"] == d:
                    changes.append((e["equnr"], "D", "is_deleted", "false", "true", ts, e["changed_by"], d))
            row["last_changed_at"], row["last_changed_by"] = ts, e["changed_by"]
    return state, changes


def build_assets(spark, names, audit, d: date) -> int:
    state, changes = asset_state(spark, names, d)
    if not state:
        return 0
    rows = [typed_asset(r) | {"as_of_date": d} for r in state.values()]
    df = spark.createDataFrame([tuple(r.get(c.split(" ")[0]) for c in ASSET_SCHEMA.split(", ")) for r in rows], ASSET_SCHEMA)
    target = names.t("silver", "erp_asset")
    if not C.table_exists(spark, target):
        C.replace_table(df, target)
    else:
        df.createOrReplaceTempView("ogx_asset_state")
        cols = [c.split(" ")[0] for c in ASSET_SCHEMA.split(", ") if not c.startswith("as_of_date")]
        differs = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in cols)
        spark.sql(f"""
            MERGE INTO {target} t USING ogx_asset_state s ON t.equnr = s.equnr
            WHEN MATCHED AND ({differs}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
    ch_t = names.t("silver", "erp_asset_change")
    C.ensure_table(spark, ch_t, CHANGE_SCHEMA, ["business_date"])
    spark.sql(f"DELETE FROM {ch_t} WHERE business_date = DATE '{d.isoformat()}'")
    if changes:
        spark.createDataFrame(changes, CHANGE_SCHEMA).writeTo(ch_t).append()
    audit.transform("apply CDC to the asset register", "standardise",
                    f"{names.t('bronze', 'erp_asset')},{names.t('bronze', 'erp_asset_cdc')}", target,
                    len(rows), len(rows), f"{len(changes)} field change(s) on {d}")
    print(f"erp_asset: {len(rows)} assets as of {d}, {len(changes)} change(s) applied", flush=True)
    return len(rows)


def build_work_orders(spark, names, audit, d: date) -> int:
    src = names.t("bronze", "erp_work_order")
    if not C.table_exists(spark, src):
        return 0
    b = spark.table(src).where(F.col("_business_date") == F.lit(d.isoformat()).cast("date"))
    w = Window.partitionBy("aufnr").orderBy(F.desc("_source_row"))
    s = (b.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
         .select("aufnr", "equnr", "tplnr", "auart", F.col("priority").cast("int").alias("priority"), "status",
                 F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
                 F.to_timestamp("closed_at", "yyyy-MM-dd HH:mm:ss").alias("closed_at"),
                 F.col("cost_usd").cast("double").alias("cost_usd"), "short_text",
                 (F.col("auart") == "PM01").alias("is_corrective"),
                 F.col("_business_date").alias("business_date"), "_source_file", "_source_row"))
    n_b, n = b.count(), s.count()
    if n == 0:
        return 0
    C.write_partitions(s, names.t("silver", "erp_work_order"), ["business_date"])
    audit.transform("type work orders", "standardise", src, names.t("silver", "erp_work_order"), n_b, n,
                    f"{n_b - n} duplicate aufnr removed")
    print(f"erp_work_order: {n_b} bronze -> {n} silver", flush=True)
    return n


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    d = args.business_date
    C.require_completed(spark, names, "bronze", d)
    audit = C.Audit(spark, names, "build_silver", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED")
    try:
        out = {"erp_asset": build_assets(spark, names, audit, d), "erp_work_order": build_work_orders(spark, names, audit, d)}
        for k, v in out.items():
            audit.load(STAGE, k, "COMMITTED", rows_out=v)
        audit.load(STAGE, "*", "COMPLETED", rows_out=sum(out.values()))
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return out


def main() -> int:
    spark = C.get_spark("ogx-silver")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
