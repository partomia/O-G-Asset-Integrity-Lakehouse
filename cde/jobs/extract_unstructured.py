"""
Stage 2 - Extract: one business date's VALID objects in bronze.doc_object, read from the raw
store (Spark binaryFile, so the bytes stay in object storage and are read on the executors), run
through the extractors of extract/ and written to silver tables. Duplicates were extracted the
first time their bytes arrived and are not read again.

  PDF report   pdf_text.parse; a page without a text layer goes through ocr.ocr_page
               -> silver.doc_page (text, method TEXT / OCR), silver.doc_chunk, silver.inspection_finding
  P&ID         drawing_tags (PDF text layer; PNG through OCR) -> silver.drawing_tag, silver.drawing_revision
  SEG-Y        segy_headers -> silver.seismic_survey, silver.seismic_trace_header
  LAS          las_curves -> silver.well_log_header, silver.well_log_curve (long: depth, mnemonic, value)
  AVI + JSON   video_keyframes + sidecar -> silver.video_keyframe (frame time, JPEG in the raw store
               at raw/frames/<sha256>.jpg, sharpness proxy bytes per kilopixel, GPS from the sidecar)
An object whose extraction fails gets a row in silver.extract_error (doc_id, reason), not a gap.

Each table replaces the date's partition (business_date) in one commit; ref.transform_log records
every extractor step with rows in and out.

  spark-submit extract_unstructured.py --business-date 2026-10-04 [--landing URI] [--raw URI]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "extract"
SCHEMAS = {
    "doc_page": "doc_id string, source string, file_name string, page_no int, method string, chars int, text string",
    "doc_chunk": "doc_id string, source string, file_name string, page_no int, chunk_no int, text string",
    "inspection_finding": (
        "doc_id string, file_name string, report_no string, inspection_ts string, facility string, equipment string, "
        "asset_class string, method string, inspector string, nominal_mm double, min_measured_mm double, "
        "wall_loss_pct double, coating string, corrosion_type string, cui string, recommended_action string, "
        "findings string, extract_method string"),
    "drawing_tag": "doc_id string, file_name string, format string, sheet string, revision string, tag string, method string",
    "seismic_survey": (
        "doc_id string, file_name string, line_name string, trace_count int, samples_per_trace int, "
        "sample_interval_us int, format_code int, record_length_ms double, first_cdp int, last_cdp int, "
        "min_x double, max_x double, min_y double, max_y double, textual_header string"),
    "seismic_trace_header": "doc_id string, line_name string, trace_no int, cdp int, x double, y double",
    "well_log_header": (
        "doc_id string, file_name string, well string, uwi string, run int, log_date string, strt double, stop double, "
        "step double, null_value double, location string, curves string, bht double"),
    "well_log_curve": "doc_id string, well string, run int, depth double, mnemonic string, unit string, value double",
    "video_keyframe": (
        "doc_id string, file_name string, video_id string, asset_hint string, frame_index int, t_seconds double, "
        "frame_sha256 string, frame_path string, size_bytes int, bytes_per_kpx double, lat double, lon double, "
        "captured_at string, fps int, duration_s double"),
    "extract_error": "doc_id string, file_name string, format string, reason string",
}


def parser():
    return C.base_parser(__doc__)


# ---------------------------------------------------------------- per object (executors)


def extract_one(meta: dict, data: bytes) -> list[tuple[str, dict]]:
    """(table, row) for one object; imports inside so executors resolve them from the mounted repo."""
    C._import_path()
    from extract import las_curves, segy_headers, video_keyframes
    from extract.chunker import chunks
    from extract.drawing_tags import tags_from_pdf, tags_from_png
    from extract.ocr import ocr_page
    from extract.pdf_text import parse as pdf_parse, report_fields

    doc, src, name, fmt = meta["doc_id"], meta["source"], meta["file_name"], meta["format"]
    out = []
    if src == "inspection" and fmt == "pdf":
        parsed = pdf_parse(data)
        texts, methods = [], []
        for p in parsed["pages"]:
            text, method = p["text"], "TEXT"
            if not text.strip() and p["image"]:
                text, method = ocr_page(p["image"]), "OCR"
            texts.append(text)
            methods.append(method)
            out.append(("doc_page", {"doc_id": doc, "source": src, "file_name": name, "page_no": p["page_no"],
                                     "method": method, "chars": len(text), "text": text}))
            for i, c in enumerate(chunks(text)):
                out.append(("doc_chunk", {"doc_id": doc, "source": src, "file_name": name, "page_no": p["page_no"],
                                          "chunk_no": i, "text": c}))
        f = report_fields("\n".join(texts))
        f["inspection_ts"] = f.pop("inspection_date")
        out.append(("inspection_finding", {"doc_id": doc, "file_name": name, **f,
                                           "extract_method": "OCR" if "OCR" in methods else "TEXT"}))
    elif src == "drawings":
        r = tags_from_pdf(data) if fmt == "pdf" else tags_from_png(data)
        for t in r["tags"]:
            out.append(("drawing_tag", {"doc_id": doc, "file_name": name, "format": fmt, "sheet": r["sheet"],
                                        "revision": r["revision"], "tag": t, "method": r["method"]}))
    elif fmt == "segy":
        s = segy_headers.parse(data)
        e = s["extent"]
        out.append(("seismic_survey", {"doc_id": doc, "file_name": name, "line_name": s["line_name"],
                                       "trace_count": s["trace_count"], "samples_per_trace": s["samples_per_trace"],
                                       "sample_interval_us": s["sample_interval_us"], "format_code": s["format_code"],
                                       "record_length_ms": s["record_length_ms"], **{k: e[k] for k in e},
                                       "textual_header": s["textual_header"]}))
        for no, cdp, x, y in s["traces"]:
            out.append(("seismic_trace_header", {"doc_id": doc, "line_name": s["line_name"], "trace_no": no,
                                                 "cdp": cdp, "x": x, "y": y}))
    elif fmt == "las":
        las = las_curves.parse(data.decode("ascii", "replace"))
        w = las["well"]
        well = w.get("WELL", "").replace("OGX ", "")
        run = int(w.get("RUN", "1") or 1)
        out.append(("well_log_header", {
            "doc_id": doc, "file_name": name, "well": well, "uwi": w.get("UWI"), "run": run, "log_date": w.get("DATE"),
            "strt": float(w["STRT"]), "stop": float(w["STOP"]), "step": float(w["STEP"]), "null_value": las["null"],
            "location": w.get("LOC"), "curves": ",".join(c[0] for c in las["curves"]),
            "bht": float(las["params"]["BHT"]) if las["params"].get("BHT") else None}))
        mnems = las["curves"]
        for row in las["rows"]:
            for (m, unit, _), v in zip(mnems[1:], row[1:]):
                if v != las["null"]:
                    out.append(("well_log_curve", {"doc_id": doc, "well": well, "run": run, "depth": row[0],
                                                   "mnemonic": m, "unit": unit, "value": v}))
    elif fmt == "avi":
        v = video_keyframes.parse(data)
        side = meta.get("sidecar") or {}
        track = side.get("gps_track") or []
        for i, k in enumerate(v["keyframes"]):
            lat, lon = (track[min(i, len(track) - 1)] if track else (None, None))
            out.append(("video_keyframe", {
                "doc_id": doc, "file_name": name, "video_id": side.get("video_id"), "asset_hint": side.get("asset_hint"),
                "frame_index": k["frame_index"], "t_seconds": k["t_seconds"], "frame_sha256": k["sha256"],
                "frame_path": None, "size_bytes": k["size"], "bytes_per_kpx": k["bytes_per_kpx"], "lat": lat,
                "lon": lon, "captured_at": side.get("captured_at"), "fps": v["fps"], "duration_s": v["duration_s"],
                "_jpeg": k["jpeg"].hex()}))
    return out


def extract_partition(rows):
    for r in rows:
        meta = json.loads(r["meta"])
        try:
            for table, row in extract_one(meta, bytes(r["content"])):
                yield table, json.dumps(row)
        except Exception as e:  # one bad object never stops the batch
            yield "extract_error", json.dumps({"doc_id": meta["doc_id"], "file_name": meta["file_name"],
                                               "format": meta["format"], "reason": f"{type(e).__name__}: {e}"[:500]})


# ---------------------------------------------------------------- driver


def drawing_revisions(spark, names, d):
    """One row per sheet revision seen up to the date, with tags added and removed vs the previous one."""
    t = names.t("silver", "drawing_tag")
    if not C.table_exists(spark, t):
        return None
    tags = (spark.table(t).where(F.col("business_date") <= F.lit(d.isoformat()).cast("date"))
            .where("format = 'pdf'").groupBy("sheet", "revision")
            .agg(F.sort_array(F.collect_set("tag")).alias("tags"), F.min("business_date").alias("effective_date"),
                 F.first("doc_id").alias("doc_id")))
    rows = sorted(tags.collect(), key=lambda r: (r["sheet"], r["revision"]))
    out, prev = [], {}
    for r in rows:
        before = prev.get(r["sheet"], [])
        out.append((r["sheet"], r["revision"], r["doc_id"], r["effective_date"], len(r["tags"]),
                    sorted(set(r["tags"]) - set(before)), sorted(set(before) - set(r["tags"])),
                    bool(before), d))
        prev[r["sheet"]] = r["tags"]
    return spark.createDataFrame(out, "sheet string, revision string, doc_id string, effective_date date, tag_count int, "
                                      "tags_added array<string>, tags_removed array<string>, is_revision boolean, "
                                      "as_of_date date")


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    d = args.business_date
    C.require_completed(spark, names, "bronze", d)
    audit = C.Audit(spark, names, "extract_unstructured", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED")
    try:
        objs = (spark.table(names.t("bronze", "doc_object"))
                .where((F.col("_business_date") == F.lit(d.isoformat()).cast("date")) & (F.col("ingest_status") == "VALID")))
        meta_rows = objs.select("doc_id", "source", "file_name", "format", "raw_path", "facts").collect()
        sidecars = {}
        fs = C.filesystem(spark, args.raw)
        for m in meta_rows:   # the drone sidecars are small JSON: read them on the driver
            if m["format"] == "json" and m["source"] == "drone":
                try:
                    sidecars[m["file_name"][:-5]] = json.loads(fs.read_bytes(m["raw_path"]))
                except Exception:
                    pass
        metas = [(m["raw_path"], json.dumps({"doc_id": m["doc_id"], "source": m["source"], "file_name": m["file_name"],
                                              "format": m["format"], "sidecar": sidecars.get(m["file_name"][:-4])}))
                 for m in meta_rows if m["format"] != "json"]
        summary = {}
        if metas:
            meta_df = spark.createDataFrame(metas, "path string, meta string")
            content = (spark.read.format("binaryFile").load([p for p, _ in metas])
                       .select(F.col("path"), "content"))
            joined = _join_paths(content, meta_df)
            pairs = joined.repartition(max(4, len(metas) // 8)).rdd.mapPartitions(extract_partition).cache()
            frames = pairs.filter(lambda kv: kv[0] == "video_keyframe").map(lambda kv: json.loads(kv[1])).collect()
            for fr in frames:   # keyframe JPEGs: stored once by hash next to the raw objects
                path = f"{args.raw}/frames/{fr['frame_sha256']}.jpg"
                if not fs.exists(path):
                    fs.write_bytes(path, bytes.fromhex(fr["_jpeg"]))
            frame_path = {fr["frame_sha256"]: f"{args.raw}/frames/{fr['frame_sha256']}.jpg" for fr in frames}
            for table, schema in SCHEMAS.items():
                if table == "video_keyframe":
                    rows = [{**{k: v for k, v in fr.items() if k != "_jpeg"}, "frame_path": frame_path[fr["frame_sha256"]]}
                            for fr in frames]
                    df = spark.createDataFrame([json.dumps(r) for r in rows], "string").toDF("j") if rows else None
                else:
                    sub = pairs.filter(lambda kv, t=table: kv[0] == t).map(lambda kv: (kv[1],))
                    df = spark.createDataFrame(sub, "j string")
                n = write_table(spark, names, table, schema, df, d)
                summary[table] = n
                if n:
                    audit.transform(f"extract {table}", "standardise", names.t("bronze", "doc_object"),
                                    names.t("silver", table), len(metas), n, f"extract/ for {table}")
            pairs.unpersist()
        rev = drawing_revisions(spark, names, d)
        if rev is not None:
            C.replace_table(rev, names.t("silver", "drawing_revision"))
            summary["drawing_revision"] = rev.count()
        audit.load(STAGE, "*", "COMPLETED", rows_in=len(metas), rows_out=sum(summary.values()),
                   rows_rejected=summary.get("extract_error", 0),
                   message=", ".join(f"{k}={v}" for k, v in summary.items() if v))
        print(f"extract {d}: {len(metas)} objects -> " + ", ".join(f"{k} {v}" for k, v in summary.items() if v), flush=True)
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return summary


def _join_paths(content, meta_df):
    """binaryFile reports fully qualified paths (file:/..., s3a://bucket/...): join on the path suffix."""
    tail = F.regexp_replace("path", r"^[a-z0-9]+:/+", "")
    return (content.withColumn("_k", tail).join(meta_df.withColumn("_k", tail).drop("path"), "_k", "inner")
            .drop("_k"))


def write_table(spark, names, table: str, schema: str, df, d) -> int:
    target = names.t("silver", table)
    if df is None:
        n = 0
    else:
        df = df.select(F.from_json("j", schema).alias("r")).select("r.*")
        df = df.withColumn("business_date", F.lit(d.isoformat()).cast("date"))
        n = df.count()
    if n == 0:
        if C.table_exists(spark, target):
            spark.sql(f"DELETE FROM {target} WHERE business_date = DATE '{d.isoformat()}'")
        return 0
    C.write_partitions(df, target, ["business_date"])
    return n


def main() -> int:
    spark = C.get_spark("ogx-extract")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
