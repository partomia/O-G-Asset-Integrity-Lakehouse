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
what) or MISMATCH. Results replace the date's rows in ref.recon_results; the mismatches are also
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
        counts = {s: sum(1 for row in r.rows if row[9] == s) for s in ("MATCHED", "EXPLAINED", "MISMATCH")}
        print(f"reconcile {r.bid}: " + ", ".join(f"{k} {v}" for k, v in counts.items()), flush=True)
        for row in bad:
            print(f"  MISMATCH {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        for row in r.rows:
            if row[9] == "EXPLAINED":
                print(f"  EXPLAINED {row[3]}.{row[4]} {row[5]}: {row[10]}", flush=True)
        audit.transform("reconcile", "validate", "ref.load_audit,bronze.*,silver.*,asset.*,gold.*",
                        names.t("ref", "recon_results"), len(r.rows), len(bad),
                        ", ".join(f"{k} {v}" for k, v in counts.items()))
        audit.load("reconcile", "*", "COMPLETED", rows_in=len(r.rows), rows_out=counts["MATCHED"] + counts["EXPLAINED"],
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
