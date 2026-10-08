"""
Stage 1 - Bronze: ingest one business date's landed files as received.

Records (the erp entities of contracts/*.json, in config/lakehouse.json order), as in GDL:
  parse      mysqldump INSERTs (column order from CREATE TABLE), pipe-delimited extracts
             (columns by header, trailer read), JSON lines (CDC)
  validate   every record against its contract (mandatory, type, date formats, pattern,
             allowed values, minimum); rejects -> bronze.quarantine with reason codes
  metadata   _batch_id, _business_date, _source_system, _source_file, _source_row,
             _ingested_at, _record_hash; PSV trailers -> ref.file_control

Objects (inspection, drone, drawings, seismic, welllogs), the unstructured pattern:
  fingerprint  sha256 of every landed file, checked against the manifest
  validate     against contracts/objects/<format>.json (PDF opens and has pages, SEG-Y headers
               readable, LAS sections present, AVI decodes and is long enough, sidecar JSON keys)
  store        copy to <raw>/<format>/<sha256>.<ext>: immutable, deduplicated by hash; bytes are
               never copied into Iceberg
  catalog      one bronze.doc_object row per file: doc_id (= sha256), source path, format, MIME,
               size, captured time, asset hint (file name or sidecar), ingest status
               (VALID / DUPLICATE / QUARANTINED); failures also -> bronze.quarantine

Each entity (and doc_object) is one Iceberg commit replacing this business date's partition,
so a re-run never duplicates rows. ref.load_audit: STARTED, COMMITTED per entity, COMPLETED
(or FAILED with the error).

Failure drill (docs/FAILED_BATCH_DEMO.md): --mode fail-during:<entity> | fail-after:<entity> | resume

Usage:
  spark-submit ingest_bronze.py --business-date 2026-10-04 [--landing URI] [--raw URI] [--db-prefix P]
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from extract.objects import format_of, validate as validate_object  # noqa: E402

STAGE = "bronze"
FILE_CONTROL_SCHEMA = ("batch_id string, business_date date, source string, entity string, source_file string, "
                       "header string, data_records bigint, trailer_records bigint, trailer_total double, "
                       "logged_at timestamp")
QUARANTINE_SCHEMA = ("_entity string, _batch_id string, _business_date date, _source_system string, "
                     "_source_file string, _source_row int, _reject_reasons array<string>, _warnings array<string>, "
                     "_record string, _record_hash string, _ingested_at timestamp")
QUARANTINE_COLS = [c.split(" ")[0] for c in QUARANTINE_SCHEMA.split(", ")]
DOC_OBJECT_SCHEMA = (
    "doc_id string, source string, source_path string, file_name string, format string, mime_type string, "
    "size_bytes bigint, sha256 string, manifest_sha256 string, captured_at timestamp, asset_hint string, "
    "hint_source string, raw_path string, ingest_status string, reject_reason string, duplicate_of string, "
    "facts string, _batch_id string, _business_date date, _ingested_at timestamp")
OBJECT_SOURCES = ("inspection", "drone", "drawings", "seismic", "welllogs")


def parser():
    p = C.base_parser(__doc__)
    p.add_argument("--mode", default="normal",
                   help="normal, resume, fail-during:<entity> or fail-after:<entity> (entity may be doc_object)")
    return p


def apply_mode(args):
    mode, _, entity = args.mode.partition(":")
    args.resume, args.fail_during, args.fail_after = mode == "resume", None, None
    if mode in ("fail-during", "fail-after") and entity:
        setattr(args, mode.replace("-", "_"), entity)
    elif mode not in ("normal", "resume"):
        raise SystemExit(f"--mode {args.mode}: use normal, resume, fail-during:<entity> or fail-after:<entity>")
    return args


# ---------------------------------------------------------------- records (copied from GDL, adapted)


def raw_frame(spark, contract: dict, folder: str) -> tuple[DataFrame, list]:
    fmt, cols = contract["format"], [c["name"] for c in contract["columns"]]
    files = spark.sparkContext.wholeTextFiles(f"{folder}/{contract['file_glob']}")
    col_schema = ", ".join(f"`{c}` string" for c in cols)
    stats = []
    if fmt == "mysqldump":
        table = contract["table"]
        rdd = files.flatMap(lambda kv, t=table, cs=cols: C.parse_dump(kv[0], kv[1], t, cs))
        df = spark.createDataFrame(rdd, f"_source_file string, _source_row int, {col_schema}")
    elif fmt == "psv":
        rdd = files.flatMap(lambda kv, cs=cols: C.parse_psv(kv[0], kv[1], cs))
        df = spark.createDataFrame(rdd, f"_source_file string, _source_row int, {col_schema}")
        stats = files.map(lambda kv: C.psv_stats(kv[0], kv[1])).collect()
    elif fmt == "jsonl":
        rdd = files.flatMap(lambda kv, cs=cols: C.parse_jsonl(kv[0], kv[1], cs))
        df = spark.createDataFrame(rdd, f"_source_file string, _source_row int, {col_schema}, "
                                        f"payload string, _parse_error string")
    else:
        raise ValueError(f"unknown format {fmt}")
    return df, stats


def _checks(columns: list, value_of) -> tuple[list, list]:
    rejects, warns = [], []
    for c in columns:
        v = F.trim(value_of(c))
        blank = v.isNull() | (v == "")
        present = ~blank
        name = c["name"]

        def add(cond, code, severity):
            (rejects if severity == "reject" else warns).append(F.when(cond, F.lit(f"{name}:{code}")))

        if c.get("required"):
            add(blank, "REQUIRED", "reject")
        kind = c["type"]
        if kind in ("int", "bigint"):
            add(present & v.cast("bigint").isNull(), "NOT_INTEGER", "reject")
        elif kind in ("decimal", "double"):
            add(present & v.cast("double").isNull(), "NOT_NUMBER", "reject")
        elif kind == "date":
            parsed = F.coalesce(*[F.to_date(v, f) for f in (c.get("formats") or [c["format"]])])
            add(present & parsed.isNull(), "INVALID_DATE", "reject")
        elif kind == "timestamp":
            add(present & F.to_timestamp(v, c["format"]).isNull(), "INVALID_TIMESTAMP", "reject")
        severity = c.get("severity", "warn")
        if "pattern" in c:
            add(present & ~v.rlike(c["pattern"]), "PATTERN", severity)
        if "allowed" in c:
            add(present & ~v.isin(c["allowed"]), "NOT_ALLOWED", severity)
        if "min" in c:
            add(present & (v.cast("double") < c["min"]), "BELOW_MIN", c.get("severity", "reject"))
    return rejects, warns


def _compact(exprs: list):
    if not exprs:
        return F.array().cast("array<string>")
    return F.filter(F.array(*exprs), lambda x: x.isNotNull())


def validate(df: DataFrame, contract: dict) -> DataFrame:
    from pyspark.sql import Window

    rejects, warns = _checks(contract["columns"], lambda c: F.col(c["name"]))
    if "payload" in df.columns:
        rejects.append(F.when(F.col("_parse_error").isNotNull(), F.lit("payload:MALFORMED_JSON")))
    warns.append(F.when(F.count(F.lit(1)).over(Window.partitionBy(*contract["key"])) > 1,
                        F.lit("key:DUPLICATE_IN_BATCH")))
    return df.withColumn("_reject_reasons", _compact(rejects)).withColumn("_warnings", _compact(warns))


def record_hash(df: DataFrame, contract: dict):
    if "payload" in df.columns:
        return F.sha2(F.col("payload"), 256)
    cols = [F.coalesce(F.col(c["name"]), F.lit("\u0000")) for c in contract["columns"]]
    return F.sha2(F.concat_ws("\u0001", *cols), 256)


def record_json(df: DataFrame, contract: dict):
    if "payload" in df.columns:
        return F.col("payload")
    return F.to_json(F.struct(*[F.col(c["name"]) for c in contract["columns"]]))


def replace_rows(spark, table: str, schema: str, where: str, rows: list) -> None:
    C.ensure_table(spark, table, schema)
    spark.sql(f"DELETE FROM {table} WHERE {where}")
    if rows:
        spark.createDataFrame(rows, schema).writeTo(table).append()


def write_batch(spark, df: DataFrame, table: str, business_date, partition_cols, fail: bool = False) -> None:
    """Replace this date's rows of `table` with df in one commit (or remove them when df is empty)."""
    if fail:
        boom = F.udf(lambda pid: C.fail_on_partition(pid), "int")
        C.write_partitions(df.repartition(4).where(boom(F.spark_partition_id()) >= 0), table, partition_cols)
        return
    if df.isEmpty():
        if C.table_exists(spark, table):
            spark.sql(f"DELETE FROM {table} WHERE _business_date = DATE '{business_date.isoformat()}'")
        return
    C.write_partitions(df, table, partition_cols)


def replace_quarantine(spark, names, df: DataFrame | None, business_date, entity: str) -> None:
    q = names.t("bronze", "quarantine")
    C.ensure_table(spark, q, QUARANTINE_SCHEMA, ["_business_date"])
    spark.sql(f"DELETE FROM {q} WHERE _business_date = DATE '{business_date.isoformat()}' AND _entity = '{entity}'")
    if df is not None and not df.isEmpty():
        df.select(*QUARANTINE_COLS).writeTo(q).append()


def ingest_entity(spark, names, audit, contract, folder, listed: dict, args) -> dict:
    entity, d, bid = contract["entity"], args.business_date, C.batch_id(args.business_date)
    started = C._now()
    target = names.t("bronze", entity)
    before = C.snapshot_id(spark, target)
    raw, stats = raw_frame(spark, contract, folder)
    meta = [F.lit(bid).alias("_batch_id"), F.lit(d).cast("date").alias("_business_date"),
            F.lit(contract["source"]).alias("_source_system"), "_source_file", "_source_row",
            F.current_timestamp().alias("_ingested_at")]
    checked = validate(raw, contract).withColumn("_record_hash", record_hash(raw, contract))
    checked = checked.withColumn("_record", record_json(raw, contract)).cache()
    data_cols = [c["name"] for c in contract["columns"]] + (["payload"] if "payload" in raw.columns else [])
    good = checked.where(F.size("_reject_reasons") == 0).select(*meta, *data_cols, "_warnings", "_record_hash")
    bad = checked.where(F.size("_reject_reasons") > 0).select(
        F.lit(entity).alias("_entity"), *meta, "_reject_reasons", "_warnings", "_record", "_record_hash")
    rows_in = checked.count()
    n_bad = bad.count()
    n_good = rows_in - n_bad
    write_batch(spark, good, target, d, ["_business_date"], fail=args.fail_during == entity)
    replace_quarantine(spark, names, bad, d, entity)
    now = C._now()
    if contract["format"] == "psv":
        replace_rows(spark, names.t("ref", "file_control"), FILE_CONTROL_SCHEMA,
                     f"batch_id = '{bid}' AND entity = '{entity}'",
                     [(bid, d, contract["source"], entity, s[0], s[1], s[2], s[3], s[4], now) for s in stats])
    reasons = (checked.where(F.size("_reject_reasons") > 0).select(F.explode("_reject_reasons").alias("r"))
               .groupBy("r").count().orderBy(F.desc("count")).limit(5).collect())
    msg = ", ".join(f"{r['r']}={r['count']}" for r in reasons)
    checked.unpersist()
    after = C.snapshot_id(spark, target)
    audit.load(STAGE, entity, "COMMITTED", rows_in=rows_in, rows_out=n_good, rows_rejected=n_bad,
               snapshot_before=before, snapshot_after=after, started_at=started,
               message=f"manifest {listed['records']}; rejected: {msg or 'none'}")
    audit.transform(f"ingest {entity}", "ingest", f"landing:{contract['source']}/{d}/{contract['file_glob']}",
                    target, rows_in, n_good, f"format {contract['format']}")
    audit.transform(f"validate {entity}", "validate", target, names.t("bronze", "quarantine"), rows_in, n_bad,
                    msg or "no rejects")
    print(f"{entity}: {rows_in} in, {n_good} accepted, {n_bad} quarantined ({msg or 'no rejects'})", flush=True)
    return {"rows_in": rows_in, "accepted": n_good, "rejected": n_bad}


# ---------------------------------------------------------------- objects

HINT_RES = {
    "inspection": re.compile(r"^IR_\d{8}_(.+)_\d{2}\.pdf$"),
    "drone": re.compile(r"^DRN_\d{8}_(.+)_\d{2}\.(?:avi|json)$"),
    "welllogs": re.compile(r"^OGX_(W-\d{2})_RUN\d+\.las$"),
    "drawings": re.compile(r"^(PID-[A-Z0-9-]+?)_REV[A-Z]\.(?:pdf|png)$"),
    "seismic": re.compile(r"^OGX_2D_(L\d+)"),
}


def asset_hint(source: str, name: str, data: bytes, fmt: str) -> tuple[str | None, str | None]:
    if source == "drone" and fmt == "json":
        try:
            return json.loads(data).get("asset_hint"), "SIDECAR"
        except ValueError:
            pass
    if source == "seismic":
        return "FLD-01", "SURVEY_AREA"
    m = HINT_RES.get(source, re.compile("$^")).match(name)
    return (m.group(1), "SHEET" if source == "drawings" else "FILE_NAME") if m else (None, None)


def captured_at(source: str, data: bytes, fmt: str, business_date) -> datetime:
    if source == "drone" and fmt == "json":
        try:
            return datetime.strptime(json.loads(data)["captured_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (ValueError, KeyError):
            pass
    return datetime.combine(business_date, datetime.min.time(), timezone.utc)


def ingest_objects(spark, names, audit, landing: str, raw_root: str, manifests: dict, args) -> dict:
    d, bid = args.business_date, C.batch_id(args.business_date)
    started = C._now()
    target = names.t("bronze", "doc_object")
    before = C.snapshot_id(spark, target)
    contracts = C.object_contracts()
    fs = C.filesystem(spark, landing)
    seen = {}
    if C.table_exists(spark, target):
        seen = {r["sha256"]: r["source_path"] for r in spark.table(target)
                .where((F.col("_business_date") < F.lit(d.isoformat()).cast("date")) & (F.col("ingest_status") == "VALID"))
                .select("sha256", "source_path").collect()}
    rows, q_rows, sidecar_capture = [], [], {}
    now = C._now()
    counts = {"VALID": 0, "DUPLICATE": 0, "QUARANTINED": 0}
    for source in OBJECT_SOURCES:
        m = manifests.get(source)
        if not m:
            continue
        folder = f"{landing}/{source}/{d.isoformat()}"
        landed = dict(fs.list(folder))
        for f in m["files"]:
            name = f["name"]
            path = f"{folder}/{name}"
            fmt = format_of(name)
            if name not in landed:
                rows.append((None, source, path, name, fmt, None, None, None, f["sha256"], None, None, None, None,
                             "QUARANTINED", "MISSING_FILE", None, "{}", bid, d, now))
                q_rows.append(("doc_object", bid, d, source, name, 1, ["object:MISSING_FILE"], [],
                               json.dumps(f), None, now))
                counts["QUARANTINED"] += 1
                continue
            data = fs.read_bytes(path)
            sha = hashlib.sha256(data).hexdigest()
            contract = contracts.get(fmt or "", {})
            ok, reason, facts = (validate_object(fmt, data, contract) if fmt else (False, "UNKNOWN_FORMAT", {}))
            if ok and sha != f["sha256"]:
                ok, reason = False, "SHA_MISMATCH"
            hint, hint_src = asset_hint(source, name, data, fmt)
            cap = captured_at(source, data, fmt, d)
            if source == "drone" and fmt == "json":
                sidecar_capture[name[:-5]] = cap
            ext = name.rsplit(".", 1)[-1].lower()
            raw_path = f"{raw_root}/{fmt}/{sha}.{ext}"
            dup_of = seen.get(sha)
            if ok and dup_of is None:
                status = "VALID"
                if not fs.exists(raw_path):
                    fs.write_bytes(raw_path, data)
                seen[sha] = path
            elif ok:
                status = "DUPLICATE"
            else:
                status = "QUARANTINED"
                q_rows.append(("doc_object", bid, d, source, name, 1, [f"object:{reason}"], [],
                               json.dumps({"sha256": sha, "size": len(data), **facts})[:4000], sha, now))
            counts[status] += 1
            rows.append([sha, source, path, name, fmt, contract.get("mime"), len(data), sha, f["sha256"], cap, hint,
                         hint_src, raw_path if status != "QUARANTINED" else None, status, reason,
                         dup_of if status == "DUPLICATE" else None, json.dumps(facts)[:4000], bid, d, now])
    for r in rows:   # a drone video takes its capture time from its sidecar
        if r[1] == "drone" and r[4] == "avi" and r[3][:-4] in sidecar_capture:
            r[9] = sidecar_capture[r[3][:-4]]
    df = spark.createDataFrame([tuple(r) for r in rows], DOC_OBJECT_SCHEMA)
    write_batch(spark, df, target, d, ["_business_date"], fail=args.fail_during == "doc_object")
    replace_quarantine(spark, names, spark.createDataFrame(q_rows, QUARANTINE_SCHEMA) if q_rows else None, d,
                       "doc_object")
    after = C.snapshot_id(spark, target)
    msg = ", ".join(f"{k}={v}" for k, v in counts.items())
    listed = sum(len((manifests.get(s) or {}).get("files", [])) for s in OBJECT_SOURCES)
    audit.load(STAGE, "doc_object", "COMMITTED", rows_in=len(rows), rows_out=counts["VALID"] + counts["DUPLICATE"],
               rows_rejected=counts["QUARANTINED"], snapshot_before=before, snapshot_after=after, started_at=started,
               message=f"manifest {listed}; {msg}")
    audit.transform("catalog objects", "ingest", f"landing:{{{','.join(OBJECT_SOURCES)}}}/{d}", target, len(rows),
                    counts["VALID"] + counts["DUPLICATE"], f"raw store {raw_root}; {msg}")
    audit.transform("validate objects", "validate", target, names.t("bronze", "quarantine"), len(rows),
                    counts["QUARANTINED"], "contracts/objects/*.json")
    audit.transform("deduplicate objects", "deduplicate", target, target, len(rows), len(rows) - counts["DUPLICATE"],
                    "by sha256 across batches")
    print(f"doc_object: {len(rows)} objects ({msg})", flush=True)
    return {"rows_in": len(rows), "accepted": counts["VALID"] + counts["DUPLICATE"], "rejected": counts["QUARANTINED"]}


# ---------------------------------------------------------------- run


def committed_since_complete(spark, names: C.Names, bid: str) -> set:
    t = names.t("ref", "load_audit")
    if not C.table_exists(spark, t):
        return set()
    scope = f"batch_id = '{bid}' AND stage = '{STAGE}'"
    return {r[0] for r in spark.sql(f"""
        SELECT DISTINCT entity FROM {t}
        WHERE {scope} AND status = 'COMMITTED'
          AND started_at > coalesce((SELECT max(started_at) FROM {t}
                                     WHERE {scope} AND entity = '*' AND status = 'COMPLETED'),
                                    TIMESTAMP '1970-01-01 00:00:00')""").collect()}


def run(spark, argv=None) -> dict:
    args = apply_mode(C.parse(parser(), argv))
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    contracts = C.contracts()
    d, bid = args.business_date, C.batch_id(args.business_date)
    landing, raw_root = C.as_uri(args.landing), C.as_uri(args.raw)
    audit = C.Audit(spark, names, "ingest_bronze", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"landing {landing}")
    fs = C.filesystem(spark, landing)
    manifests = C.read_manifests(fs, landing, d)
    skip = committed_since_complete(spark, names, bid) if args.resume else set()
    summary = {}
    try:
        for source, m in manifests.items():
            if m is None and source in ("erp", "inspection", "drone"):
                audit.load(STAGE, f"{source}:*", "MISSING_MANIFEST", message=f"no {source} manifest for {d}")
        for entity, contract in contracts.items():
            m = manifests.get(contract["source"]) or {}
            files = [f for f in m.get("files", []) if f.get("entity") == entity]
            if not files:
                continue
            if entity in skip:
                print(f"{entity}: already committed for {bid}, skipped (--resume)")
                audit.load(STAGE, entity, "SKIPPED", message="committed by an earlier attempt (--resume)")
                continue
            listed = {"records": sum(f.get("records", 0) for f in files)}
            folder = f"{landing}/{contract['source']}/{d.isoformat()}"
            summary[entity] = ingest_entity(spark, names, audit, contract, folder, listed, args)
            if args.fail_after == entity:
                raise RuntimeError(f"simulated failure after committing {entity} (--fail-after)")
        if "doc_object" in skip:
            audit.load(STAGE, "doc_object", "SKIPPED", message="committed by an earlier attempt (--resume)")
        else:
            summary["doc_object"] = ingest_objects(spark, names, audit, landing, raw_root, manifests, args)
            if args.fail_after == "doc_object":
                raise RuntimeError("simulated failure after committing doc_object (--fail-after)")
        audit.load(STAGE, "*", "COMPLETED", rows_in=sum(s["rows_in"] for s in summary.values()),
                   rows_out=sum(s["accepted"] for s in summary.values()),
                   rows_rejected=sum(s["rejected"] for s in summary.values()))
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return summary


def main() -> int:
    spark = C.get_spark("ogx-ingest-bronze")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
