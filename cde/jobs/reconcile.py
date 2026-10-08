"""
Reconciliation: one business date, every layer built so far, against the sources' own control
figures (batch manifests, file trailers, stream control messages), never against what Spark
happened to read. Copied from GDL and adapted.

  bronze   load_status      every manifest entity COMMITTED by a COMPLETED bronze run
           landed_records   manifest record count = bronze accepted + quarantined
           control_total    manifest control total = accepted + quarantined values (work order cost)
           file_trailer     PSV trailer count = data lines in the file (source-side defect)
           landed_objects   manifest files = bronze.doc_object rows, per source
           object_quarantine  0 objects rejected (a corrupt or mislabelled file is a source defect)
           object_duplicates  resends of identical bytes: EXPLAINED, stored once
  stream   stream_records   producer control count = bronze sensor_reading + stream_quarantine, per
                            topic and event minute summed over the date; late rows EXPLAINED
  silver   silver_*         bronze to silver, EXPLAINED by duplicates removed or rows not applicable
  gold     gold_*           silver to facts, dimension links valid on the date

Each check is MATCHED, EXPLAINED (the difference is fully accounted for, and the detail says by
what), LATE (stream rows still arriving, or past the watermark) or MISMATCH. Results replace the date's rows in ref.recon_results; the mismatches are also
written as a CSV report under <reports>/recon/<date>/. --fail-on-mismatch makes a MISMATCH fail the
job (a gate in the DAG).

Usage:
  spark-submit reconcile.py --business-date 2026-10-04 [--landing URI] [--db-prefix P]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

RESULT_SCHEMA = ("run_id string, batch_id string, business_date date, layer string, entity string, "
                 "check_name string, expected double, actual double, difference double, status string, "
                 "detail string, checked_at timestamp_ntz")   # Impala writes it too
OBJECT_SOURCES = ("inspection", "drone", "drawings", "seismic", "welllogs")
TOLERANCE = 0.005
CHECKS = []   # extra layers register here: fn(recon, manifests)


def parser():
    p = C.base_parser(__doc__)
    p.add_argument("--reports", default=None, help="reports folder (default: next to --landing)")
    p.add_argument("--fail-on-mismatch", action="store_true")
    p.add_argument("--layers", default="bronze,stream,silver,asset,gold")
    return p


class Recon:
    def __init__(self, spark, names, business_date):
        self.spark, self.names, self.d = spark, names, business_date
        self.bid = C.batch_id(business_date)
        self.rows: list[tuple] = []
        self.run_id = None

    def add(self, layer, entity, check, expected, actual, status=None, detail="", explained_by=None):
        expected = None if expected is None else float(expected)
        actual = None if actual is None else float(actual)
        diff = None if expected is None or actual is None else round(actual - expected, 2)
        if status is None:
            if diff is not None and abs(diff) <= TOLERANCE:
                status = "MATCHED"
            elif explained_by is not None and diff is not None and abs(diff - explained_by) <= TOLERANCE:
                status = "EXPLAINED"
            else:
                status = "MISMATCH"
        self.rows.append((self.run_id, self.bid, self.d, layer, entity, check, expected, actual, diff, status,
                          detail, C._now().replace(tzinfo=None)))

    def table(self, layer, name):
        t = self.names.t(layer, name)
        return self.spark.table(t) if C.table_exists(self.spark, t) else None

    def on_date(self, df, col="_business_date"):
        return df.where(F.col(col) == F.lit(self.d.isoformat()).cast("date"))


def _sum(df, col):
    v = df.agg(F.sum(F.col(col).cast("double"))).collect()[0][0] if df is not None else None
    return round(v or 0.0, 2)


# ---------------------------------------------------------------- bronze


def latest_run(r: Recon, stage: str = "bronze") -> tuple[str, str, dict]:
    """Status and message of the batch's latest run of a stage, and entity -> COMMITTED/SKIPPED in it."""
    audit = r.table("ref", "load_audit")
    if audit is None:
        return "NOT RUN", "", {}
    b = audit.where((F.col("batch_id") == r.bid) & (F.col("stage") == stage))
    start = b.where("entity = '*' AND status = 'STARTED'").orderBy(F.desc("started_at")).limit(1).collect()
    if not start:
        return "NOT RUN", "", {}
    rows = b.where(F.col("run_id") == start[0]["run_id"]).collect()
    end = [x for x in rows if x["entity"] == "*" and x["status"] in ("COMPLETED", "FAILED")]
    status, message = (end[0]["status"], end[0]["message"] if end[0]["status"] == "FAILED" else "") if end \
        else ("RUNNING", "")
    return status, message, {x["entity"]: x["status"] for x in rows if x["status"] in ("COMMITTED", "SKIPPED")}


def bronze_checks(r: Recon, manifests: dict) -> None:
    run_status, run_message, by_entity = latest_run(r)
    r.add("bronze", "*", "batch_status", 1, 1 if run_status == "COMPLETED" else 0,
          "MATCHED" if run_status == "COMPLETED" else "MISMATCH",
          f"last bronze run {run_status}" + (f": {run_message}" if run_message else ""))
    quarantine = r.table("bronze", "quarantine")
    for entity, contract in C.contracts().items():
        files = [f for f in (manifests.get(contract["source"]) or {}).get("files", []) if f.get("entity") == entity]
        if not files:
            continue
        listed = sum(f.get("records", 0) for f in files)
        t = r.table("bronze", entity)
        accepted = r.on_date(t) if t is not None else None
        q = r.on_date(quarantine).where(F.col("_entity") == entity) if quarantine is not None else None
        n_acc = accepted.count() if accepted is not None else 0
        n_q = q.count() if q is not None else 0
        load_status(r, entity, by_entity.get(entity), run_status, n_acc)
        r.add("bronze", entity, "landed_records", listed, n_acc + n_q,
              detail=f"{n_acc} accepted + {n_q} quarantined vs {listed} in the manifest")
        ctl = [f["control_total"] for f in files if f.get("control_total") is not None]
        field = contract.get("control_field")
        if ctl and field:
            acc_sum = _sum(accepted, field)
            q_sum = round((q.agg(F.sum(F.get_json_object("_record", f"$.{field}").cast("double"))).collect()[0][0]
                           or 0.0) if q is not None else 0.0, 2)
            r.add("bronze", entity, "control_total", sum(ctl), acc_sum + q_sum,
                  detail=f"accepted {acc_sum:,.2f} + quarantined {q_sum:,.2f} vs manifest {sum(ctl):,.2f} ({field})")
    fc = r.table("ref", "file_control")
    if fc is not None:
        for row in fc.where(F.col("batch_id") == r.bid).collect():
            if row["trailer_records"] is None:
                continue
            r.add("bronze", row["entity"], "file_trailer", row["trailer_records"], row["data_records"],
                  detail=f"{row['source_file']}: trailer says {row['trailer_records']}, "
                         f"file has {row['data_records']} data lines (source-side defect when they differ)")
    object_checks(r, manifests, by_entity.get("doc_object"), run_status)


def load_status(r: Recon, entity: str, state, run_status: str, n_acc: int) -> None:
    if state == "COMMITTED":
        r.add("bronze", entity, "load_status", 1, 1, "MATCHED", f"committed by the last run ({run_status})")
    elif state == "SKIPPED":
        r.add("bronze", entity, "load_status", 1, 1, "MATCHED", "committed by an earlier attempt (--resume)")
    else:
        held = f"the table still holds {n_acc} rows for the date from an earlier run" if n_acc else "no rows for the date"
        r.add("bronze", entity, "load_status", 1, 0, "MISMATCH", f"not loaded by the last run ({run_status}); {held}")


def object_checks(r: Recon, manifests: dict, state, run_status: str) -> None:
    t = r.table("bronze", "doc_object")
    objs = r.on_date(t) if t is not None else None
    listed_any = any((manifests.get(s) or {}).get("files") for s in OBJECT_SOURCES)
    if not listed_any:
        return
    load_status(r, "doc_object", state, run_status, objs.count() if objs is not None else 0)
    counts = {}
    if objs is not None:
        for x in objs.groupBy("source", "ingest_status").count().collect():
            counts.setdefault(x["source"], {})[x["ingest_status"]] = x["count"]
        reasons = {}
        for x in objs.where("ingest_status = 'QUARANTINED'").select("source", "file_name", "reject_reason").collect():
            reasons.setdefault(x["source"], []).append(f"{x['file_name']} {x['reject_reason']}")
    for source in OBJECT_SOURCES:
        m = manifests.get(source)
        if not m or not m.get("files"):
            continue
        c = counts.get(source, {})
        n = sum(c.values())
        r.add("bronze", f"doc_object:{source}", "landed_objects", len(m["files"]), n,
              detail=f"{c.get('VALID', 0)} valid + {c.get('DUPLICATE', 0)} duplicate + {c.get('QUARANTINED', 0)} "
                     f"quarantined vs {len(m['files'])} in the manifest")
        nq = c.get("QUARANTINED", 0)
        r.add("bronze", f"doc_object:{source}", "object_quarantine", 0, nq,
              detail=("rejected at bronze: " + "; ".join(reasons.get(source, [])) + " (source-side defect)")
              if nq else "every object passed its format contract")
        nd = c.get("DUPLICATE", 0)
        if nd:
            r.add("bronze", f"doc_object:{source}", "object_duplicates", 0, nd, "EXPLAINED",
                  f"{nd} resend(s) of bytes already received (same sha256): catalogued, stored once")


# ---------------------------------------------------------------- stream


STREAM_TABLES = {"ogx.sensor.telemetry": ("sensor_reading", "event_id"), "ogx.scada.alarm": ("scada_alarm", "alarm_id")}


def stream_checks(r: Recon, _manifests: dict) -> None:
    """Producer control counts (ogx.control, per event minute, topic and producer run) against the
    rows bronze landed for the same minutes; then bronze against the 1-minute windows of silver."""
    ctl_t = r.table("bronze", "stream_control")
    if ctl_t is None:
        return
    day = F.lit(r.d.isoformat()).cast("date")
    ctl = (ctl_t.where(F.to_date("minute") == day)
           .dropDuplicates(["minute", "topic", "producer_run"]).select("minute", "topic", "producer_run", "total"))
    if ctl.isEmpty():
        return
    q_t = r.table("bronze", "stream_quarantine")
    newest = ctl.agg(F.max("minute")).collect()[0][0]
    for topic, (table, key) in STREAM_TABLES.items():
        t = r.table("bronze", table)
        expected = ctl.where(F.col("topic") == topic)
        n_expected = _sum(expected, "total")
        if t is None:
            r.add("stream", table, "stream_records", n_expected, 0, detail=f"bronze.{table} does not exist")
            continue
        rows = t.where(F.col("event_date") == day)
        landed = (rows.groupBy(F.date_trunc("minute", "event_ts").alias("minute"), "producer_run")
                  .agg(F.countDistinct(key).alias("landed"), F.count("*").alias("rows")))
        if q_t is not None:
            qd = (q_t.where((F.col("kafka_topic") == topic) & (F.col("event_date") == day))
                  .select(F.date_trunc("minute", F.to_timestamp(F.get_json_object("record_value", "$.event_ts"))).alias("minute"),
                          F.get_json_object("record_value", "$.producer_run").alias("producer_run"))
                  .groupBy("minute", "producer_run").agg(F.count("*").alias("quarantined")))
        else:
            qd = None
        per_min = expected.join(landed, ["minute", "producer_run"], "left")
        if qd is not None:
            per_min = per_min.join(qd, ["minute", "producer_run"], "left")
        else:
            per_min = per_min.withColumn("quarantined", F.lit(0))
        per_min = per_min.fillna(0, ["landed", "rows", "quarantined"]).cache()
        got = per_min.agg(F.sum("landed"), F.sum("quarantined"), F.sum("rows")).collect()[0]
        n_landed, n_q, n_rows = int(got[0] or 0), int(got[1] or 0), int(got[2] or 0)
        short = per_min.where(F.col("landed") + F.col("quarantined") < F.col("total"))
        recent = short.where(F.col("minute") >= F.lit(newest) - F.expr("INTERVAL 10 MINUTES")).count()
        n_short = short.count()
        detail = (f"{n_landed} landed + {n_q} quarantined vs {int(n_expected)} in the producer's control messages "
                  f"({expected.count()} minutes); {n_short} minute(s) short")
        if n_short and n_short == recent:
            r.add("stream", table, "stream_records", n_expected, n_landed + n_q, "LATE",
                  detail + " (all within the last 10 minutes: still arriving)")
        else:
            r.add("stream", table, "stream_records", n_expected, n_landed + n_q, detail=detail)
        r.add("stream", table, "stream_duplicates", 0, n_rows - n_landed,
              detail=f"{n_rows - n_landed} row(s) with a repeated {key} (exactly-once across restarts)")
        per_min.unpersist()
    w = r.table("silver", "sensor_window")
    rd = r.table("bronze", "sensor_reading")
    if w is None or rd is None:
        return
    n_b = rd.where(F.col("event_date") == day).count()
    n_w = int(w.where((F.col("window_size") == "1 minute") & (F.col("window_date") == day))
              .agg(F.sum("n")).collect()[0][0] or 0)
    very_late = rd.where((F.col("event_date") == day) & (F.col("lateness_s") > 600)).count()
    dropped = n_b - n_w
    if dropped and 0 < dropped <= very_late:
        r.add("stream", "sensor_window", "window_records", n_b, n_w, "LATE",
              f"bronze {n_b}, 1-minute windows {n_w}: {dropped} reading(s) arrived after the 10-minute watermark "
              f"({very_late} sent more than 10 minutes late) and were not windowed")
    else:
        r.add("stream", "sensor_window", "window_records", n_b, n_w,
              detail=f"bronze {n_b} readings, 1-minute windows hold {n_w} ({very_late} sent > 10 minutes late, "
                     f"windowed while their window's state was open)")


CHECKS.append(("stream", stream_checks))


# ---------------------------------------------------------------- silver

EXTRACT_OUTPUTS = {"pdf:inspection": ["inspection_finding"], "pdf:drawings": ["drawing_tag"], "png:drawings": ["drawing_tag"],
                   "segy:seismic": ["seismic_survey"], "las:welllogs": ["well_log_header"], "avi:drone": ["video_keyframe"]}


def silver_checks(r: Recon, _manifests: dict) -> None:
    b, s = r.table("bronze", "erp_work_order"), r.table("silver", "erp_work_order")
    if b is not None and s is not None:
        b, s = r.on_date(b), r.on_date(s, "business_date")
        n_b, n_s = b.count(), s.count()
        dupes = n_b - b.dropDuplicates(["aufnr"]).count()
        r.add("silver", "erp_work_order", "silver_records", n_b, n_s, explained_by=-dupes,
              detail=f"bronze {n_b} accepted, {dupes} duplicate aufnr, silver {n_s}")
        r.add("silver", "erp_work_order", "silver_total", _sum(b, "cost_usd"), _sum(s, "cost_usd"),
              detail="work order cost, bronze vs silver (USD)")
    objs = r.table("bronze", "doc_object")
    if objs is None:
        return
    valid = r.on_date(objs).where("ingest_status = 'VALID' AND format <> 'json'").select("doc_id", "format", "source")
    errors = r.table("silver", "extract_error")
    err_ids = {x["doc_id"] for x in r.on_date(errors, "business_date").select("doc_id").collect()} if errors is not None else set()
    by_kind = {}
    for x in valid.collect():
        by_kind.setdefault(f"{x['format']}:{x['source']}", set()).add(x["doc_id"])
    for kind, ids in sorted(by_kind.items()):
        found = set()
        for table in EXTRACT_OUTPUTS.get(kind, []):
            t = r.table("silver", table)
            if t is not None:
                found |= {x["doc_id"] for x in r.on_date(t, "business_date").select("doc_id").distinct().collect()}
        hit, err = len(ids & found), len(ids & err_ids)
        r.add("silver", f"extract:{kind}", "extracted_objects", len(ids), hit, explained_by=-err if err else None,
              detail=f"{len(ids)} valid object(s), {hit} with extracted rows, {err} in silver.extract_error")
    f = r.table("silver", "inspection_finding")
    if f is not None:
        f = r.on_date(f, "business_date")
        missing = f.where("report_no IS NULL OR equipment IS NULL OR wall_loss_pct IS NULL").count()
        r.add("silver", "inspection_finding", "fields_extracted", 0, missing,
              detail=f"{missing} report(s) without report number, equipment or wall loss "
                     f"({f.where(F.col('extract_method') == 'OCR').count()} read by OCR)")


CHECKS.append(("silver", silver_checks))


# ---------------------------------------------------------------- output


def write(r: Recon, reports: str) -> list:
    spark, target = r.spark, r.names.t("ref", "recon_results")
    C.ensure_table(spark, target, RESULT_SCHEMA, ["business_date"])
    layers = sorted({row[3] for row in r.rows})
    if layers:
        in_list = ", ".join(f"'{x}'" for x in layers)
        spark.sql(f"DELETE FROM {target} WHERE batch_id = '{r.bid}' AND layer IN ({in_list})")
        spark.createDataFrame(r.rows, RESULT_SCHEMA).writeTo(target).append()
    bad = [row for row in r.rows if row[9] == "MISMATCH"]
    out = f"{reports}/recon/{r.d.isoformat()}"
    fs = C.filesystem(spark, out)
    lines = ["layer,entity,check,expected,actual,difference,detail"]
    lines += [",".join(str(x) if x is not None else "" for x in row[3:9]) + ',"' + row[10].replace('"', "'") + '"'
              for row in bad]
    fs.write_bytes(f"{out}/mismatch_report.csv", ("\n".join(lines) + "\n").encode())
    return bad


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    landing = C.as_uri(args.landing)
    reports = C.as_uri(args.reports) if args.reports else C.reports_for(landing)
    layers = set(args.layers.split(","))
    audit = C.Audit(spark, names, "reconcile", args.business_date, args.pipeline_run)
    r = Recon(spark, names, args.business_date)
    r.run_id = audit.run_id
    audit.load("reconcile", "*", "STARTED")
    try:
        manifests = C.read_manifests(C.filesystem(spark, landing), landing, args.business_date)
        if "bronze" in layers:
            bronze_checks(r, manifests)
        for layer, fn in CHECKS:
            if layer in layers:
                fn(r, manifests)
        bad = write(r, reports)
        counts = {s: sum(1 for row in r.rows if row[9] == s) for s in ("MATCHED", "EXPLAINED", "LATE", "MISMATCH")}
        print(f"reconcile {r.bid}: " + ", ".join(f"{k} {v}" for k, v in counts.items()), flush=True)
        for row in bad:
            print(f"  MISMATCH {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        for row in r.rows:
            if row[9] in ("EXPLAINED", "LATE"):
                print(f"  {row[9]} {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        audit.transform("reconcile", "validate", "ref.load_audit,bronze.*,silver.*,asset.*,gold.*",
                        names.t("ref", "recon_results"), len(r.rows), len(bad),
                        ", ".join(f"{k} {v}" for k, v in counts.items()))
        audit.load("reconcile", "*", "COMPLETED", rows_in=len(r.rows), rows_out=counts["MATCHED"] + counts["EXPLAINED"] + counts["LATE"],
                   rows_rejected=counts["MISMATCH"], message=f"report {reports}/recon/{r.d.isoformat()}/")
        if bad and args.fail_on_mismatch:
            raise RuntimeError(f"{len(bad)} reconciliation mismatch(es) for {r.bid}")
    finally:
        audit.flush()
    return counts


def main() -> int:
    spark = C.get_spark("ogx-reconcile")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
