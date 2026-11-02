"""
What the agents see: one facility on one business date, read from the lakehouse through the same
Impala views as the Workbench (certified worklist, gold facts, silver sensor windows and report
chunks). `query` is any callable sql -> list[dict], so tests run on fixtures.

Nothing classified OGX_SENSITIVE_* (frame hashes, coordinates) is selected: it never reaches a prompt.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
LH = json.loads((ROOT / "config" / "lakehouse.json").read_text())
CFG = json.loads((ROOT / "config" / "inference.json").read_text())
P = os.environ.get("OGX_DB_PREFIX", LH.get("db_prefix", "rsingh_ogx"))
FACILITIES = {"FLD1": "Onshore field FLD-01, 12 producing wells",
              "PL03": "Export pipeline PL-03, 60 segments",
              "REF1": "Refinery unit REF-U1"}
Query = Callable[[str], list[dict]]


def lit(s) -> str:
    return "'" + str(s).replace("'", "") + "'"


def _clean(v):
    if isinstance(v, float):
        return None if math.isnan(v) else round(v, 2)
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ")[:16] if isinstance(v, datetime) else v.isoformat()
    return v


def rows_json(rows: list[dict], cols: list[str] | None = None) -> str:
    out = [{k: _clean(r.get(k)) for k in (cols or r.keys()) if r.get(k) is not None} for r in rows]
    return json.dumps(out, separators=(",", ":"), default=str)


@dataclass
class Scope:
    facility: str
    business_date: str
    worklist: list[dict]
    windows: list[dict]
    known_tags: set[str]
    daily: list[dict] = field(default_factory=list)
    sensor_daily: list[dict] = field(default_factory=list)
    inspections: list[dict] = field(default_factory=list)
    work_orders: list[dict] = field(default_factory=list)
    doc_chunks: list[dict] = field(default_factory=list)
    focus: list[str] = field(default_factory=list)

    @property
    def facility_name(self) -> str:
        return FACILITIES.get(self.facility, self.facility)

    @property
    def scope_tags(self) -> set[str]:
        return {r["tag"] for r in self.worklist}

    @property
    def abstained(self) -> set[str]:
        return {r["tag"] for r in self.worklist if r.get("is_abstained")}

    @property
    def asset_by_tag(self) -> dict[str, dict]:
        return {r["tag"]: r for r in self.worklist}

    @property
    def doc_files(self) -> set[str]:
        return {c["file_name"] for c in self.doc_chunks}

    def focus_rows(self, rows: list[dict], key: str = "tag") -> list[dict]:
        f = set(self.focus)
        return [r for r in rows if r.get(key) in f]

    def blocks(self) -> dict[str, str]:
        wl_cols = ["risk_rank", "tag", "asset_class", "criticality", "risk_band", "wall_loss_pct", "breach_windows_3d",
                   "corrective_wo_30d", "days_overdue", "p_event_30d", "exposure", "abstain_reason"]
        excerpts = "\n".join(f"[{c['file_name']} p.{c['page_no']}] {c['text']}" for c in self.doc_chunks) or "None."
        return {
            "facility": self.facility, "facility_name": self.facility_name, "business_date": self.business_date,
            "worklist": rows_json(self.focus_rows(self.worklist), wl_cols),
            "sensor_daily": rows_json(self.sensor_daily, ["tag", "sensor_tag", "measurement", "readings", "mean_value",
                                                          "max_value", "mean_slope_per_h", "breach_windows",
                                                          "stuck_windows", "late_readings", "bar_readings"]),
            "inspections": rows_json(self.inspections, ["tag", "report_no", "file_name", "inspected_at", "method",
                                                        "nominal_mm", "min_measured_mm", "wall_loss_pct", "coating",
                                                        "corrosion_type", "cui", "recommended_action"]),
            "doc_excerpts": excerpts,
            "work_orders": rows_json(self.work_orders, ["tag", "aufnr", "auart", "priority", "status", "created_at",
                                                        "short_text", "is_corrective"]),
            "work_order_days": str(CFG["scope"]["work_order_days"]),
        }


def load(query: Query, facility: str, business_date: str, asset_tag: str | None = None) -> Scope:
    d = f"DATE {lit(business_date)}"
    wl = query(f"SELECT risk_rank, tag, asset_id, asset_class, criticality, risk_band, wall_loss_pct, breach_windows_3d, "
               f"corrective_wo_30d, days_overdue, rule_score, p_event_30d, exposure, is_abstained, abstain_reason "
               f"FROM {P}_semantic.mis_risk_worklist WHERE business_date = {d} AND facility_id = {lit(facility)} "
               f"ORDER BY risk_rank")
    win = query(f"SELECT w.sensor_tag, w.measurement, w.window_start, w.mean_value, w.max_value, w.std_value, "
                f"w.is_stuck, w.is_breach, w.n_bar, w.n_late, w.high_threshold, a.tag AS asset_tag "
                f"FROM {P}_silver.sensor_window w "
                f"JOIN (SELECT DISTINCT sensor_tag, asset_id FROM {P}_gold.fact_sensor_daily WHERE business_date = {d}) s "
                f"ON s.sensor_tag = w.sensor_tag "
                f"JOIN {P}_gold.dim_asset a ON a.asset_id = s.asset_id AND a.is_current "
                f"WHERE w.window_date = {d} AND w.window_size = {lit(CFG['prescreen']['window_size'])} "
                f"AND a.facility_id = {lit(facility)}"
                + (f" AND a.tag = {lit(asset_tag)}" if asset_tag else ""))
    daily = query(f"SELECT a.tag, s.asset_id, s.sensor_tag, s.measurement, s.readings, s.mean_value, s.max_value, "
                  f"s.mean_slope_per_h, s.breach_windows, s.stuck_windows, s.late_readings, s.bar_readings "
                  f"FROM {P}_gold.fact_sensor_daily s JOIN {P}_gold.dim_asset a ON a.asset_id = s.asset_id AND a.is_current "
                  f"WHERE s.business_date = {d} AND a.facility_id = {lit(facility)}"
                  + (f" AND a.tag = {lit(asset_tag)}" if asset_tag else "") + " ORDER BY a.tag, s.sensor_tag")
    known = {r["tag"] for r in query(f"SELECT tag FROM {P}_gold.dim_asset WHERE is_current")}
    known |= {r["sensor_tag"].removesuffix(".PV") for r in
              query(f"SELECT DISTINCT sensor_tag FROM {P}_gold.fact_sensor_daily WHERE business_date = {d}")}
    if asset_tag:
        wl = [r for r in wl if r["tag"] == asset_tag]
    return Scope(facility, business_date, wl, win, known, daily=daily)


def enrich(query: Query, scope: Scope, flagged: list[str]) -> Scope:
    """Focus = assets the pre-screen flagged plus the top of the worklist; pull their evidence."""
    top = CFG["scope"]["worklist_top"]
    focus = list(dict.fromkeys([t for t in flagged if t in scope.scope_tags] + [r["tag"] for r in scope.worklist[:top]]))
    scope.focus = focus
    by_tag = scope.asset_by_tag
    ids = [by_tag[t]["asset_id"] for t in focus if t in by_tag]
    if not ids:
        return scope
    d = f"DATE {lit(scope.business_date)}"
    inlist = ", ".join(lit(i) for i in ids)
    tag_of = {by_tag[t]["asset_id"]: t for t in focus if t in by_tag}

    sd = [r for r in scope.daily if r["asset_id"] in set(ids)]
    insp = query(f"SELECT asset_id, doc_id, report_no, file_name, inspected_at, `method`, nominal_mm, min_measured_mm, "
                 f"wall_loss_pct, coating, corrosion_type, cui, recommended_action FROM {P}_gold.fact_inspection "
                 f"WHERE asset_id IN ({inlist}) AND business_date <= {d} ORDER BY inspected_at DESC")
    since = (date.fromisoformat(scope.business_date) - timedelta(days=CFG["scope"]["work_order_days"])).isoformat()
    wo = query(f"SELECT asset_id, aufnr, auart, priority, status, created_at, short_text, is_corrective "
               f"FROM {P}_gold.fact_work_order WHERE asset_id IN ({inlist}) AND business_date <= {d} "
               f"AND created_at >= CAST({lit(since)} AS TIMESTAMP) ORDER BY created_at DESC")
    latest: dict[str, dict] = {}
    for r in insp:
        latest.setdefault(r["asset_id"], r)
    for rows in (list(latest.values()), wo):
        for r in rows:
            r["tag"] = tag_of.get(r["asset_id"], r["asset_id"])
    scope.sensor_daily, scope.inspections, scope.work_orders = sd, list(latest.values()), wo[:40]

    docs = [r["doc_id"] for r in latest.values() if r.get("doc_id")]
    if docs:
        n = CFG["scope"]["doc_chunks_per_asset"]
        scope.doc_chunks = query(f"SELECT doc_id, file_name, page_no, chunk_no, text FROM {P}_silver.doc_chunk "
                                 f"WHERE doc_id IN ({', '.join(lit(x) for x in docs)}) AND chunk_no < {int(n)} "
                                 f"ORDER BY file_name, page_no, chunk_no")
    return scope
