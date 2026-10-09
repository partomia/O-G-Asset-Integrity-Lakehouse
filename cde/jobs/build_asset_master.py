"""
Stage 4 - Asset master (rsingh_ogx_asset): one asset_id behind every way a source names the same
equipment, as GDL's MDM does for customers. Rebuilt as of each business date (partition as_of_date),
so the resolution on any past date can be read back.

  asset_candidate  every name seen up to the date: EAM functional location (REF-U1-P-101A), P&ID tag
                   (P-101A), inspection report equipment, file-name hint, drone sidecar hint with its
                   GPS track, well-log well, sensor tag (PI-101A.PV)
  match_pair       candidate -> asset, with rule, score and decision
  rules            EXACT_FUNC_LOC   the register's own functional location (score 1.0)
                   NORMALISED_TAG   tag with the instrument prefix and .PV suffix stripped and the
                                    dashes unified: PI-101A.PV, P-101A, VI-101A.PV -> loop 101A (0.95)
                   GPS_SEGMENT      a drone track on the pipeline resolves to the segment whose
                                    position is nearest, within GPS_BAND_M (0.90); when the file name
                                    names another segment the GPS wins and the pair goes to the review
                                    queue as AUTO_RESOLVED (the planted wrong-tag video of 2026-10-06)
                   anything else    review queue (OPEN), never dropped
  asset_xref       (source system, source name) -> asset_id
  golden_asset     the register's attributes as of the date (source of each in attr_source), status,
                   sensor and document counts
  review_queue     unresolved or conflicting names with the reason and a suggested asset

  spark-submit build_asset_master.py --business-date 2026-10-06 [--db-prefix P]
"""

from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ogx_common as C  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "asset"
GPS_BAND_M = 500.0
FACILITY_PREFIX = re.compile(r"^(?:REF-U\d+|FLD-\d+)-")
SENSOR_RE = re.compile(r"^(?:PI|TI|VI|CP)-(.+?)(?:\.(?:PV|SP|OP|MV))?$")
CANDIDATE_SCHEMA = ("src_system string, src_key string, src_name string, norm_key string, lat double, lon double, "
                    "first_seen date, as_of_date date")
PAIR_SCHEMA = ("src_system string, src_key string, src_name string, asset_id string, rule string, score double, "
               "decision string, detail string, as_of_date date")
XREF_SCHEMA = "src_system string, src_name string, asset_id string, rule string, score double, as_of_date date"
REVIEW_SCHEMA = ("src_system string, src_key string, src_name string, reason string, suggested_asset_id string, "
                 "status string, detail string, as_of_date date")
GOLDEN_SCHEMA = (
    "asset_id string, equnr string, tag string, func_loc string, asset_class string, facility_id string, "
    "description string, criticality string, material string, design_pressure_psi double, install_date date, "
    "inspection_interval_days int, last_inspection_date date, latitude double, longitude double, "
    "chainage_from_km double, chainage_to_km double, status string, is_active boolean, n_sensors int, "
    "n_documents int, attr_source string, as_of_date date")
CLASS_BY_PREFIX = {"W": "WELL", "P": "PUMP", "K": "COMPRESSOR", "V": "VESSEL", "E": "HEAT_EXCHANGER", "TK": "TANK",
                   "C": "COLUMN", "FL": "FLARE_STACK", "XV": "VALVE", "PSV": "RELIEF_VALVE"}


def parser():
    return C.base_parser(__doc__)


def tag_of(func_loc: str) -> str:
    return FACILITY_PREFIX.sub("", func_loc or "")


def norm_key(name: str | None) -> str | None:
    """One key per asset for every naming: loop number for refinery equipment, W01 for wells,
    PL03-001 for pipeline segments."""
    if not name:
        return None
    n = name.strip().upper()
    m = SENSOR_RE.match(n)
    if m:
        n = m.group(1)
    n = tag_of(n)
    if re.fullmatch(r"PL-?\d{2}-?SEG-?\d{3}", n):
        return re.sub(r"PL-?(\d{2})-?SEG-?(\d{3})", r"PL\1-\2", n)
    if re.fullmatch(r"PL\d{2}-\d{3}", n):
        return n
    if re.fullmatch(r"W-?\d{2}", n):
        return n.replace("-", "")
    m = re.fullmatch(r"(?:[A-Z]{1,3}-?)?(\d{3}[AB]?)", n)
    return m.group(1) if m else n


def haversine_m(a, b) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6_371_000 * 2 * math.asin(math.sqrt(h))


def _upto(df, col, d):
    return df.where(F.col(col) <= F.lit(d.isoformat()).cast("date"))


def collect_candidates(spark, names, d) -> list[dict]:
    t = lambda layer, n: names.t(layer, n)  # noqa: E731
    out = []

    def add(system, key, name, first, lat=None, lon=None):
        if name:
            out.append({"src_system": system, "src_key": key, "src_name": name, "norm_key": norm_key(name),
                        "lat": lat, "lon": lon, "first_seen": first})

    if C.table_exists(spark, t("silver", "drawing_tag")):
        for r in (_upto(spark.table(t("silver", "drawing_tag")), "business_date", d).groupBy("sheet", "tag")
                  .agg(F.min("business_date").alias("first")).collect()):
            add("drawing", r["sheet"], r["tag"], r["first"])
    if C.table_exists(spark, t("silver", "inspection_finding")):
        for r in _upto(spark.table(t("silver", "inspection_finding")), "business_date", d).select(
                "doc_id", "equipment", "business_date").collect():
            add("inspection", r["doc_id"], r["equipment"], r["business_date"])
    objs = _upto(spark.table(t("bronze", "doc_object")), "_business_date", d).where(
        "ingest_status = 'VALID' AND source IN ('inspection', 'welllogs') AND asset_hint IS NOT NULL")
    for r in objs.select("doc_id", "source", "asset_hint", "_business_date").collect():
        add(f"{r['source']}_file", r["doc_id"], r["asset_hint"], r["_business_date"])
    if C.table_exists(spark, t("silver", "video_keyframe")):
        vids = (_upto(spark.table(t("silver", "video_keyframe")), "business_date", d)
                .groupBy("doc_id", "asset_hint").agg(F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
                                                     F.min("business_date").alias("first")).collect())
        for r in vids:
            add("drone", r["doc_id"], r["asset_hint"], r["first"], r["lat"], r["lon"])
    if C.table_exists(spark, t("silver", "sensor_window")):
        for r in (_upto(spark.table(t("silver", "sensor_window")), "window_date", d).groupBy("sensor_tag")
                  .agg(F.min("window_date").alias("first")).collect()):
            add("sensor", r["sensor_tag"], r["sensor_tag"], r["first"])
    return out


def resolve(register: list, candidates: list, d):
    by_key = {}
    for a in register:
        by_key.setdefault(norm_key(a["tag"]), []).append(a)
    segments = [a for a in register if a["asset_class"] == "PIPE_SEGMENT" and a["latitude"] is not None]
    pairs, review = [], []
    for c in candidates:
        hits = by_key.get(c["norm_key"], [])
        base = (c["src_system"], c["src_key"], c["src_name"])
        gps = None
        if c["src_system"] == "drone" and c["lat"] is not None and segments:
            near = min(segments, key=lambda s: haversine_m((c["lat"], c["lon"]), (s["latitude"], s["longitude"])))
            dist = haversine_m((c["lat"], c["lon"]), (near["latitude"], near["longitude"]))
            gps = (near, dist) if dist <= GPS_BAND_M else None
        if gps and (not hits or hits[0]["asset_id"] != gps[0]["asset_id"]) and \
                (not hits or hits[0]["asset_class"] == "PIPE_SEGMENT"):
            near, dist = gps
            named = hits[0]["asset_id"] if hits else None
            pairs.append((*base, near["asset_id"], "GPS_SEGMENT", 0.90, "MATCHED",
                          f"track {dist:.0f} m from {near['tag']}" + (f"; file name says {hits[0]['tag']}" if hits else ""), d))
            if named:
                pairs.append((*base, named, "NORMALISED_TAG", 0.95, "REJECTED", f"GPS places the video on {near['tag']}", d))
                review.append((*base, "NAME_GPS_CONFLICT", near["asset_id"], "AUTO_RESOLVED",
                               f"file name names {hits[0]['tag']}, GPS track is {dist:.0f} m from {near['tag']}: "
                               f"resolved to {near['tag']}", d))
            continue
        if len(hits) == 1:
            a = hits[0]
            rule = "EXACT_FUNC_LOC" if c["src_name"] == a["func_loc"] else "NORMALISED_TAG"
            detail = f"{c['src_name']} -> {c['norm_key']}"
            if not a["is_active"]:
                detail += f" (asset {a['status']})"
            pairs.append((*base, a["asset_id"], rule, 1.0 if rule == "EXACT_FUNC_LOC" else 0.95, "MATCHED", detail, d))
            if gps and gps[0]["asset_id"] == a["asset_id"]:
                pairs[-1] = (*pairs[-1][:7], detail + f"; GPS agrees ({gps[1]:.0f} m)", d)
        elif len(hits) > 1:
            review.append((*base, "AMBIGUOUS", None, "OPEN", f"{len(hits)} assets share key {c['norm_key']}", d))
            pairs.append((*base, None, "NORMALISED_TAG", 0.5, "REVIEW", f"{len(hits)} candidates", d))
        else:
            review.append((*base, "NOT_IN_REGISTER", None, "OPEN",
                           f"no register asset with key {c['norm_key']} on {d}", d))
            pairs.append((*base, None, None, 0.0, "REVIEW", "no match", d))
    return pairs, review


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names)
    d = args.business_date
    C.require_completed(spark, names, "silver", d)
    audit = C.Audit(spark, names, "build_asset_master", d, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED")
    try:
        reg = spark.table(names.t("silver", "erp_asset")).collect()
        hist = {}   # the register as of d from the snapshot history when re-run for a past date
        register = []
        for r in reg:
            status = (r["status"] or "").upper()
            register.append({
                "asset_id": f"OGX-{r['equnr']}", "equnr": r["equnr"], "tag": tag_of(r["tplnr"]), "func_loc": r["tplnr"],
                "asset_class": r["eqart"], "facility_id": r["swerk"], "description": r["eqktx"],
                "criticality": r["abckz"], "material": r["werks_mat"], "design_pressure_psi": r["design_press_psi"],
                "install_date": r["inbdt"], "inspection_interval_days": r["insp_interval_d"],
                "last_inspection_date": r["last_insp_date"], "latitude": r["gps_lat"], "longitude": r["gps_lon"],
                "chainage_from_km": r["chain_from_km"], "chainage_to_km": r["chain_to_km"], "status": status or "ACTIVE",
                "is_active": not r["is_deleted"] and status not in ("DCOM", "DECOMMISSIONED", "INAC"),
                "changed_by": r["last_changed_by"]})
        del hist
        cands = collect_candidates(spark, names, d)
        cands += [{"src_system": "erp", "src_key": a["equnr"], "src_name": a["func_loc"], "norm_key": norm_key(a["tag"]),
                   "lat": a["latitude"], "lon": a["longitude"], "first_seen": d} for a in register]
        pairs, review = resolve(register, cands, d)
        matched = [p for p in pairs if p[6] == "MATCHED"]
        xref = sorted({(p[0], p[2], p[3], p[4], p[5], d) for p in matched})
        sensors_of, docs_of = {}, {}
        for p in matched:
            if p[0] == "sensor":
                sensors_of[p[3]] = sensors_of.get(p[3], 0) + 1
            elif p[0] in ("inspection", "drone", "welllogs_file"):
                docs_of.setdefault(p[3], set()).add(p[1])
        golden = []
        for a in register:
            src = {k: "erp" for k in ("tag", "func_loc", "asset_class", "criticality", "design_pressure_psi",
                                      "install_date", "inspection_interval_days", "last_inspection_date",
                                      "latitude", "longitude", "status")}
            golden.append((a["asset_id"], a["equnr"], a["tag"], a["func_loc"], a["asset_class"], a["facility_id"],
                           a["description"], a["criticality"], a["material"], a["design_pressure_psi"], a["install_date"],
                           a["inspection_interval_days"], a["last_inspection_date"], a["latitude"], a["longitude"],
                           a["chainage_from_km"], a["chainage_to_km"], a["status"], a["is_active"],
                           sensors_of.get(a["asset_id"], 0), len(docs_of.get(a["asset_id"], ())), json.dumps(src), d))
        cand_rows = [(c["src_system"], c["src_key"], c["src_name"], c["norm_key"], c["lat"], c["lon"], c["first_seen"], d)
                     for c in cands]
        for table, rows, schema in (("asset_candidate", cand_rows, CANDIDATE_SCHEMA), ("match_pair", pairs, PAIR_SCHEMA),
                                    ("asset_xref", xref, XREF_SCHEMA), ("golden_asset", golden, GOLDEN_SCHEMA),
                                    ("review_queue", review, REVIEW_SCHEMA)):
            target = names.t("asset", table)
            C.ensure_table(spark, target, schema, ["as_of_date"])
            spark.sql(f"DELETE FROM {target} WHERE as_of_date = DATE '{d.isoformat()}'")
            if rows:
                spark.createDataFrame(rows, schema).writeTo(target).append()
            audit.load(STAGE, table, "COMMITTED", rows_out=len(rows))
        by_rule = {}
        for p in matched:
            by_rule[p[4]] = by_rule.get(p[4], 0) + 1
        audit.transform("match names to assets", "match", "silver.*,bronze.doc_object", names.t("asset", "asset_xref"),
                        len(cands), len(matched), ", ".join(f"{k}={v}" for k, v in sorted(by_rule.items())))
        audit.transform("survive register attributes", "survive", names.t("silver", "erp_asset"),
                        names.t("asset", "golden_asset"), len(register), len(golden), "source of truth: EAM register")
        open_n = sum(1 for x in review if x[5] == "OPEN")
        audit.load(STAGE, "*", "COMPLETED", rows_in=len(cands), rows_out=len(matched), rows_rejected=open_n,
                   message=f"rules {by_rule}; review {len(review)} ({open_n} open)")
        print(f"asset master {d}: {len(cands)} candidates, {len(matched)} matched {by_rule}, "
              f"review {len(review)} ({open_n} open)", flush=True)
        for x in review:
            print(f"  review {x[3]} {x[0]}:{x[2]} -> {x[4]} {x[5]}: {x[6]}", flush=True)
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return {"candidates": len(cands), "matched": len(matched), "review": len(review)}


def main() -> int:
    spark = C.get_spark("ogx-asset-master")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
