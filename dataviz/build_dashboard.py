#!/usr/bin/env python3
"""
The four O&G Asset Integrity dashboards in Cloudera Data Visualization, as code (copied from
GDL's build_dashboard.py; the export/import/verify machinery is unchanged):

  OGX Integrity KPIs             integrity risk exposure and the risk-ranked worklist,
                                 unstructured coverage, severe-defect time to review
  OGX Reconciliation & Data Quality  every reconciliation check per layer (bronze, stream,
                                 silver, semantic) and the load audit
  OGX Asset Master               how every source name resolved to a golden asset, and the review queue
  OGX Sensor Health              per-sensor breaches, stuck sensors and unit changes from the stream

Datasets, visuals and sheets are declared below; this script turns them into one Data
Visualization export file (dataviz/ogx_dashboards.json) with fixed UUIDs and primary keys
(13000+), so an import updates the dashboards in place. Every dataset is a view in
rsingh_ogx_semantic (sql/semantic/20_mis_views.sql, 50_dashboard_views.sql).

  set -a; source .env; set +a
  python dataviz/build_dashboard.py                          # write the file (column types from Impala)
  python dataviz/build_dashboard.py --import --connection federal-impala-1
  python dataviz/build_dashboard.py --check                  # every visual's query directly on Impala
  python dataviz/build_dashboard.py --verify                 # every visual through the Data API
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

OUT = ROOT / "dataviz" / "ogx_dashboards.json"
DB = "rsingh_ogx_semantic"
NS = uuid.UUID("8e2b4c1d-6a73-4f05-b9d8-1c3e5a7f9b20")
DASHBOARD_PK0 = 13000
DATASET_PK0 = 13100
VISUAL_PK0 = 13200
DEFAULT_VERSION = {"Arcviz Version": "8.1.4.1000", "Description": "8.1.4.1000-4"}

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "risk_fac": ("OGX - Risk by facility", "mis_risk_by_facility", {"is_latest"}),
    "worklist": ("OGX - Risk worklist", "mis_risk_worklist", {"is_latest", "risk_rank", "is_abstained"}),
    "coverage": ("OGX - Unstructured coverage", "mis_coverage_by_format", {"is_latest"}),
    "ttr": ("OGX - Time to review", "mis_time_to_review", {"is_latest"}),
    "recon": ("OGX - Reconciliation", "dash_recon", {"is_latest"}),
    "audit": ("OGX - Load audit", "dash_load_audit", {"is_latest"}),
    "xref": ("OGX - Asset cross-reference", "dash_asset_master", {"is_latest"}),
    "review": ("OGX - Asset review queue", "dash_review_queue", {"is_latest"}),
    "sensor": ("OGX - Sensor daily", "dash_sensor_daily", {"is_latest"}),
}

LATEST = "[is_latest] = 1"
pct = lambda e: f"round(100 * {e}, 1)"  # noqa: E731

KPI_SHEETS = [
    ("Integrity risk", [
        dict(type="kpi", ds="risk_fac", title="Integrity risk exposure", measures=[("round(sum([exposure]), 2)", "Exposure")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="risk_fac", title="High-risk assets", measures=[("sum([high_risk_assets])", "High")],
             filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="risk_fac", title="Assets abstained (stuck sensor)",
             measures=[("sum([abstained_assets])", "Abstained")], filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="coverage", title="Unstructured coverage %",
             measures=[(pct("sum([covered]) / sum([received])"), "Coverage %")], filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="risk_fac", title="Integrity risk exposure per business date and facility",
             x=[("business_date", "Business date")], measures=[("sum([exposure])", "Exposure")],
             color=[("facility_id", "Facility")], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="worklist", title="Assets by risk band (latest date)",
             x=[("risk_band", "Risk band")], measures=[("sum(1)", "Assets")], color=[("asset_class", "Class")],
             filters=[LATEST], pos=(33, 11, 32, 22)),
        dict(type="table", ds="worklist", title="Risk-ranked worklist (latest date, top 25)",
             dims=[("risk_rank", "Rank"), ("tag", "Tag"), ("asset_class", "Class"), ("facility_id", "Facility"),
                   ("criticality", "Criticality"), ("risk_band", "Band"), ("abstain_reason", "Abstained")],
             measures=[("max([wall_loss_pct])", "Wall loss %"), ("max([breach_windows_3d])", "Breach windows 3d"),
                       ("max([corrective_wo_30d])", "Corrective WOs 30d"), ("max([rule_score])", "Score"),
                       ("max([exposure])", "Exposure")],
             filters=[LATEST, "[risk_rank] <= 25"], sort_dim="risk_rank", pos=(1, 33, 64, 30)),
    ]),
    ("Coverage and review time", [
        dict(type="trellis-bars", ds="coverage", title="Objects received and covered by format (all dates)",
             x=[("format", "Format")], measures=[("sum([received])", "Received"), ("sum([covered])", "Covered")],
             pos=(1, 1, 32, 22)),
        dict(type="trellis-lines", ds="coverage", title="Coverage % per business date",
             x=[("business_date", "Business date")],
             measures=[(pct("sum([covered]) / sum([received])"), "Coverage %")], pos=(33, 1, 32, 22)),
        dict(type="trellis-bars", ds="ttr", title="Severe-defect hours to review: risk order vs calendar order",
             x=[("business_date", "Business date")], measures=[("avg([avg_hours])", "Avg hours")],
             color=[("worklist_order", "Worklist order")], pos=(1, 23, 64, 22)),
        dict(type="table", ds="ttr", title="Time to review per date and order",
             dims=[("business_date", "Business date"), ("worklist_order", "Order")],
             measures=[("sum([severe_defects])", "Severe defects"), ("avg([avg_hours])", "Avg hours"),
                       ("max([max_hours])", "Max hours")], sort_dim="business_date", pos=(1, 45, 64, 20)),
    ]),
]

RECON_SHEETS = [
    ("Latest batch", [
        dict(type="kpi", ds="recon", title="Checks on the latest batch", measures=[("sum(1)", "Checks")],
             filters=[LATEST], pos=(1, 1, 13, 10)),
        dict(type="kpi", ds="recon", title="Matched", measures=[("sum([is_ok])", "Matched")],
             filters=[LATEST], pos=(14, 1, 13, 10)),
        dict(type="kpi", ds="recon", title="Explained", measures=[("sum([is_explained])", "Explained")],
             filters=[LATEST], pos=(27, 1, 13, 10)),
        dict(type="kpi", ds="recon", title="Late (stream, past watermark)", measures=[("sum([is_late])", "Late")],
             filters=[LATEST], pos=(40, 1, 13, 10)),
        dict(type="kpi", ds="recon", title="Mismatches", measures=[("sum([is_mismatch])", "Mismatches")],
             filters=[LATEST], pos=(53, 1, 12, 10)),
        dict(type="trellis-bars", ds="recon", title="Checks by layer (latest batch)",
             x=[("layer_label", "Layer")], measures=[("sum([is_ok])", "Matched"), ("sum([is_explained])", "Explained"),
                                                     ("sum([is_late])", "Late"), ("sum([is_mismatch])", "Mismatch")],
             filters=[LATEST], pos=(1, 11, 24, 22)),
        dict(type="table", ds="recon", title="Explained, late and mismatched checks, with the reason",
             dims=[("layer_label", "Layer"), ("entity", "Entity"), ("check_name", "Check"), ("status", "Status"),
                   ("detail", "Detail")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([difference])", "Difference")],
             filters=[LATEST, "[status] <> 'MATCHED'"], may_be_empty=True, sort_dim="layer_label",
             pos=(25, 11, 40, 22)),
    ]),
    ("By business date", [
        dict(type="trellis-bars", ds="recon", title="Checks per business date",
             x=[("business_date", "Business date")], measures=[("sum([is_ok])", "Matched"),
                                                               ("sum([is_explained])", "Explained"),
                                                               ("sum([is_mismatch])", "Mismatch")],
             pos=(1, 1, 64, 22)),
        dict(type="table", ds="recon", title="Every mismatch (the planted corrupt PDF and trailer error)",
             dims=[("business_date", "Business date"), ("layer_label", "Layer"), ("entity", "Entity"),
                   ("check_name", "Check"), ("detail", "Detail")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([difference])", "Difference")],
             filters=["[status] = 'MISMATCH'"], may_be_empty=True, sort_dim="business_date", sort_asc=False,
             pos=(1, 23, 64, 22)),
    ]),
    ("Load audit", [
        dict(type="kpi", ds="audit", title="Failed attempts (all batches)", measures=[("sum([is_failed])", "Failed")],
             pos=(1, 1, 32, 10)),
        dict(type="kpi", ds="audit", title="Rows rejected (latest batch)", measures=[("sum([rows_rejected])", "Rejected")],
             filters=[LATEST, "[entity] <> '*'"], pos=(33, 1, 32, 10)),
        dict(type="trellis-bars", ds="audit", title="Rows written per business date and stage",
             x=[("business_date", "Business date")], measures=[("sum([rows_out])", "Rows")],
             color=[("stage", "Stage")], filters=["[status] = 'COMMITTED'"], pos=(1, 11, 64, 22)),
        dict(type="table", ds="audit", title="Stage results of the latest batch",
             dims=[("stage", "Stage"), ("entity", "Entity"), ("status", "Status")],
             measures=[("sum([rows_in])", "Rows in"), ("sum([rows_out])", "Rows out"),
                       ("sum([rows_rejected])", "Rejected")],
             filters=[LATEST], sort_dim="stage", pos=(1, 33, 64, 26)),
    ]),
]

ASSET_SHEETS = [
    ("Resolution", [
        dict(type="kpi", ds="xref", title="Source names resolved (latest)", measures=[("sum(1)", "Resolved")],
             filters=[LATEST], pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="review", title="Conflicts auto-resolved (latest)",
             measures=[("sum(1)", "Conflicts")], filters=[LATEST], may_be_empty=True, pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="review", title="Open in the review queue", measures=[("sum([is_open])", "Open")],
             filters=[LATEST], may_be_empty=True, pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="xref", title="Source names by resolving rule and system (latest)",
             x=[("rule_label", "Rule")], measures=[("sum(1)", "Names")], color=[("src_system", "Source")],
             filters=[LATEST], pos=(1, 11, 64, 22)),
        dict(type="table", ds="review", title="Review queue: conflicts and how they resolved",
             dims=[("business_date", "Business date"), ("src_system", "Source"), ("src_name", "Name"),
                   ("reason", "Reason"), ("suggested_asset_id", "Asset"), ("status", "Status"), ("detail", "Detail")],
             measures=[("sum(1)", "Rows")], may_be_empty=True, sort_dim="business_date", pos=(1, 33, 64, 22)),
    ]),
]

SENSOR_SHEETS = [
    ("Sensors", [
        dict(type="kpi", ds="sensor", title="Sensors reporting (latest)", measures=[("sum(1)", "Sensors")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="sensor", title="Readings (latest)", measures=[("sum([readings])", "Readings")],
             filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="sensor", title="Stuck sensors (latest)", measures=[("sum([is_stuck])", "Stuck")],
             filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="sensor", title="Sensors reporting in bar (unit change)",
             measures=[("sum([has_unit_change])", "Unit change")], filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="sensor", title="Breach windows per business date and measurement",
             x=[("business_date", "Business date")], measures=[("sum([breach_windows])", "Breach windows")],
             color=[("measurement", "Measurement")], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="sensor", title="Late readings per business date",
             x=[("business_date", "Business date")], measures=[("sum([late_readings])", "Late readings")],
             pos=(33, 11, 32, 22)),
        dict(type="table", ds="sensor", title="Sensors needing attention (latest): stuck, unit change or breaching",
             dims=[("sensor_tag", "Sensor"), ("measurement", "Measurement"), ("tag", "Asset"),
                   ("asset_class", "Class"), ("facility_id", "Facility")],
             measures=[("sum([stuck_windows])", "Stuck windows"), ("sum([bar_readings])", "Bar readings"),
                       ("sum([breach_windows])", "Breach windows"), ("max([max_value])", "Max")],
             filters=[LATEST, "([stuck_windows] > 0 or [bar_readings] > 0 or [breach_windows] > 0)"],
             may_be_empty=True, sort_dim="sensor_tag", pos=(1, 33, 64, 26)),
    ]),
]

DASHBOARDS = [
    dict(title="OGX Integrity KPIs", pk=DASHBOARD_PK0, key="kpi", sheets=KPI_SHEETS, main_ds="risk_fac",
         subtitle="Integrity risk exposure, risk-ranked worklist, unstructured coverage and severe-defect time to review"),
    dict(title="OGX Reconciliation & Data Quality", pk=DASHBOARD_PK0 + 1, key="reconciliation", sheets=RECON_SHEETS,
         main_ds="recon", subtitle="Reconciliation per layer and batch, KPI consistency, load audit"),
    dict(title="OGX Asset Master", pk=DASHBOARD_PK0 + 2, key="asset-master", sheets=ASSET_SHEETS, main_ds="xref",
         subtitle="ERP, inspection, drawing, sensor and drone names resolved to one golden asset"),
    dict(title="OGX Sensor Health", pk=DASHBOARD_PK0 + 3, key="sensor", sheets=SENSOR_SHEETS, main_ds="sensor",
         subtitle="Streaming telemetry per sensor and day: breaches, stuck sensors, unit changes, late readings"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def visuals_of(d: dict):
    for sheet, items in d["sheets"]:
        for v in items:
            yield sheet, v


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def impala():
    import run_semantic as S

    return S.ImpalaEngine(json.loads((ROOT / "config" / "lakehouse.json").read_text())["impala"])


def column_types(engine, ds_key: str) -> dict[str, str]:
    cols, rows = engine.query(f"DESCRIBE {DB}.{DATASETS[ds_key][1]}")
    return {r[cols.index("name")]: str(r[cols.index("type")]).upper() for r in rows}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int, dashboards: list[int]) -> dict:
    name, view, _ = DATASETS[key]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/semantic)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": dashboards,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "general-datalakehouse", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_record(v: dict, pk: int, sheet: str, types: dict[str, str], dataset_pk: int, dash: dict) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_desc"):
        shelves["y_shelf"][0]["order"] = {"priority": 1, "ascending": False}
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": 1, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", dash["key"], sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def dashboard_record(d: dict, pk0: int, visuals: list[dict], ds_pk: dict[str, int], types: dict) -> dict:
    sheets, pk = [], pk0
    for order, (sheet, items) in enumerate(d["sheets"], 1):
        placed = []
        for v in items:
            pk += 1
            visuals.append(visual_record(v, pk, sheet, types[v["ds"]], ds_pk[v["ds"]], d))
            placed.append((pk, v["pos"]))
        sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                       "visual_widgets": [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{p}"}
                                          for i, (p, (c, r, w, h)) in enumerate(placed, 1)],
                       "control_widgets": []})
    dash = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
            "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
            "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                              "permit_csv_download_dashboard": "true"},
            "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
    return {"model": "reports.report", "pk": d["pk"], "fields": {
        "report_name": d["title"], "report_description": "docs/DATAVIZ.md",
        "dataset": ds_pk[d["main_ds"]], "workspace": 1, "report_type": "dashboard", "report_mode": None,
        "dashboard_url_name": "", "report_data": json.dumps(dash), "shared_visual_dashboards": "[]",
        "parent_report": None, "uuid": uid("dashboard", d["key"]), "imported_uuid": None, "has_css_styles": False,
        "report_search_text": None}}


def missing_columns(types: dict[str, dict[str, str]]) -> list[str]:
    out = []
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            exprs = [e for e, _ in v["measures"]] + v.get("filters", [])
            cols = {c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])}
            cols |= {c for e in exprs for c in re.findall(r"\[(\w+)\]", e)}
            out += [f"{d['title']} / {sheet} / {v['title']}: {c}" for c in sorted(cols - set(types[v["ds"]]))]
    return out


def build(types: dict[str, dict[str, str]], conn_id: int, version: dict) -> dict:
    missing = missing_columns(types)
    if missing:
        raise SystemExit("columns not in the dataset's view:\n  " + "\n  ".join(missing))
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    visuals, dashboards, pk0 = [], [], VISUAL_PK0
    for d in DASHBOARDS:
        dashboards.append(dashboard_record(d, pk0, visuals, ds_pk, types))
        pk0 += 100
    used_by = {k: [d["pk"] for d in DASHBOARDS if any(v["ds"] == k for _, v in visuals_of(d))] for k in DATASETS}
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id, used_by[k]) for k in DATASETS],
            "version": version}


def _strip(e: str) -> str:
    return re.sub(r"\[(\w+)\]", r"\1", e)


def tile_sql(v: dict) -> str:
    """A KPI tile's number as plain Impala SQL on its view ([col] -> col)."""
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    return f"SELECT {_strip(v['measures'][0][0])} FROM {DB}.{DATASETS[v['ds']][1]} WHERE {where}"


def visual_sql(v: dict) -> str:
    """The query a visual sends, as plain Impala SQL."""
    dims = [c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])]
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    group = f" GROUP BY {', '.join(dims)}" if dims else ""
    return (f"SELECT {', '.join(dims + [_strip(e) for e, _ in v['measures']])} FROM {DB}.{DATASETS[v['ds']][1]} "
            f"WHERE {where}{group} LIMIT {v.get('limit', 1000)}")


def check(engine) -> int:
    """Every visual's query straight on Impala: the expressions parse and the visual has rows."""
    failed = 0
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            try:
                rows = engine.query(visual_sql(v))[1]
                ok, note = bool(rows) or v.get("may_be_empty", False), f"{len(rows)} rows"
                if rows and v["type"] == "kpi":
                    note = f"{rows[0][-1]}"
            except Exception as e:  # noqa: BLE001
                ok, note = False, str(e).strip().splitlines()[-1][:200]
            failed += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {d['title']} / {sheet} / {v['title']}: {note}", flush=True)
    return failed


class DataViz:
    """The CDW Data Visualization instance, authenticated with a Data Visualization API key."""

    def __init__(self):
        import requests

        key = os.environ.get("OGX_VIZ_API_KEY")
        if not key:
            raise SystemExit("set OGX_VIZ_API_KEY (Data Visualization: Site Administration -> Manage API Keys), "
                             "or import dataviz/ogx_dashboards.json in the UI")
        self.url = json.loads((ROOT / "config" / "lakehouse.json").read_text())["dataviz_url"].rstrip("/")
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"apikey {key}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"GET {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def connections(self) -> list[dict]:
        return self.get("/arc/adminapi/v1/connections")

    def connection_id(self, name: str) -> int:
        found = next((c for c in self.connections() if c["name"] == name), None)
        if found is None:
            raise SystemExit(f"no connection {name!r}; --list-connections shows them")
        return found["id"]

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def import_file(self, path: Path, connection: str) -> None:
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": connection}, timeout=300)
        print(f"import: HTTP {r.status_code} {r.text[:500]}")
        if r.status_code != 200:
            raise SystemExit(1)

    def verify(self, engine) -> int:
        """Every visual's query through the Data API (Data Visualization -> its connection -> Impala);
        each KPI tile's number is also computed directly in Impala and compared."""
        ids = {d["name"]: d["id"] for d in self.get("/arc/adminapi/v1/datasets")}
        failed = 0
        for d in DASHBOARDS:
            for sheet, v in visuals_of(d):
                failed += not self.verify_visual(v, f"{d['title']} / {sheet}", ids, engine)
        return failed

    def verify_visual(self, v: dict, where: str, ids: dict, engine) -> bool:
        dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
        dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                 "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                 "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                 "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
        r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)}, timeout=300)
        rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
        ok = bool(rows) or (rows is not None and v.get("may_be_empty", False))
        note = f"{len(rows)} rows" if rows is not None else f"HTTP {r.status_code} {r.text[:200]}"
        if ok and v["type"] == "kpi":
            want = engine.query(tile_sql(v))[1][0][0]
            got = rows[0][-1] if isinstance(rows[0], list) else list(rows[0].values())[-1]
            ok = abs(float(got) - float(want)) < 1e-6
            note = f"{got} (Impala {want})"
        print(f"{'ok  ' if ok else 'FAIL'} {where} / {v['title']}: {note}")
        return ok


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--import", dest="do_import", action="store_true", help="import the file (needs OGX_VIZ_API_KEY)")
    p.add_argument("--connection", help="Data Visualization connection to the Impala warehouse (with --import)")
    p.add_argument("--verify", action="store_true", help="run every visual's query through the Data API")
    p.add_argument("--check", action="store_true", help="run every visual's query directly on Impala")
    p.add_argument("--list-connections", action="store_true")
    args = p.parse_args()
    if args.list_connections:
        for c in DataViz().connections():
            print(c["id"], c["name"], c.get("type"))
        return 0
    engine = impala()
    if args.check:
        return 1 if check(engine) else 0
    if args.verify:
        return 1 if DataViz().verify(engine) else 0
    viz = DataViz() if args.do_import else None
    if viz and not args.connection:
        p.error("--import needs --connection (see --list-connections)")
    types = {k: column_types(engine, k) for k in DATASETS}
    conn_id = viz.connection_id(args.connection) if viz else 1
    doc = build(types, conn_id, viz.version() if viz else DEFAULT_VERSION)
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['dashboards'])} dashboards, {len(doc['datasets'])} datasets, "
          f"{len(doc['visuals'])} visuals, {sum(len(d['sheets']) for d in DASHBOARDS)} sheets")
    if viz:
        viz.import_file(OUT, args.connection)
        print(f"open {viz.url}/arc/apps/ -> Dashboards -> " + " / ".join(d["title"] for d in DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
