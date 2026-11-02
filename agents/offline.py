"""
Rule-based stand-in for the three agents when Cloudera AI Inference is unreachable or the
circuit is open: the same stages and ticket blocks, written from the scope and the pre-screen
alone. Labelled "rule-based" wherever it is shown; it never pretends to be the LLM.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from agents.context import Scope
from agents.prescreen import Prescreen

HIGH_WALL_LOSS = 25.0
MAX_TICKETS = (yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "guardrails.yaml").read_text())
               .get("llm", {}).get("max_tickets", 8))


def _f(v, nd=1) -> str:
    return "n/a" if v is None else f"{float(v):.{nd}f}"


def _immediate(scope: Scope) -> list[dict]:
    insp = {r["tag"]: r for r in scope.inspections}
    out = []
    for r in scope.focus_rows(scope.worklist):
        i = insp.get(r["tag"])
        wl = (i or {}).get("wall_loss_pct") or r.get("wall_loss_pct")
        if (wl is not None and float(wl) >= HIGH_WALL_LOSS) or r.get("risk_band") == "HIGH":
            out.append({**r, "insp": i, "wl": wl})
    return out


def condition(scope: Scope, pre: Prescreen) -> str:
    wl = scope.worklist
    high = [r for r in wl if r.get("risk_band") == "HIGH"]
    exp = sum(float(r.get("exposure") or 0) for r in wl)
    lines = ["_Rule-based summary: Cloudera AI Inference unavailable._", "", "### Facility condition",
             f"- {len(wl)} assets ranked on {scope.business_date}; {len(high)} in band HIGH; integrity risk exposure "
             f"{exp:.2f}; {len(scope.abstained)} abstained for a stuck sensor."]
    for r in wl[:3]:
        lines.append(f"- Rank {r['risk_rank']}: {r['tag']} ({r['asset_class']}, criticality {r['criticality']}), band "
                     f"{r['risk_band']}, wall loss {_f(r.get('wall_loss_pct'))} %, exposure {_f(r.get('exposure'), 2)}.")
    lines += ["", "### Flagged assets"]
    lines += [f"- {s.line()}" for s in pre.signals if s.kind in ("zscore", "breach")] or ["- None."]
    lines += ["", "### Data faults"]
    lines += [f"- {s.line()}" for s in pre.signals if s.kind in ("stuck", "unit_change")] or ["- None."]
    return "\n".join(lines)


def integrity(scope: Scope, pre: Prescreen) -> str:
    lines = ["### Immediate risk"]
    for r in _immediate(scope):
        i = r["insp"] or {}
        cite = f" [{i['file_name']} p.1]" if i.get("file_name") in scope.doc_files else ""
        lines.append(f"- {r['tag']}: wall loss {_f(r['wl'])} %, {str(i.get('corrosion_type') or 'mechanism not reported').lower()}"
                     f", CUI {i.get('cui') or 'n/a'}; report recommends: {i.get('recommended_action') or 'n/a'}{cite}")
    if len(lines) == 1:
        lines.append("- None.")
    lines += ["", "### Watchlist"]
    lines += [f"- {s.line()}" for s in pre.signals if s.kind in ("zscore", "breach")] or ["- None."]
    lines += ["", "### Sensor faults"]
    lines += [f"- {s.sensor_tag} on {s.asset_tag}: {s.kind.replace('_', ' ')}, readings not trusted until repaired"
              for s in pre.signals if s.kind in ("stuck", "unit_change")] or ["- None."]
    return "\n".join(lines)


def plan(scope: Scope, pre: Prescreen, max_tickets: int = MAX_TICKETS) -> str:
    tickets: list[str] = []
    seen: set[str] = set()

    def add(tag, ttype, prio, due, desc, ev):
        if tag in seen or tag not in scope.scope_tags or len(tickets) >= max_tickets:
            return
        seen.add(tag)
        tickets.append(f"```ticket\nasset_tag: {tag}\nticket_type: {ttype}\npriority: {prio}\ndue_in_days: {due}\n"
                       f"description: {desc}\nevidence: {ev}\n```")

    for s in pre.signals:
        if s.kind in ("stuck", "unit_change"):
            add(s.asset_tag, "Sensor", "Medium", 7, f"Check and repair transmitter {s.sensor_tag}; "
                f"{'flat-lined' if s.kind == 'stuck' else 'reporting in bar instead of psi'}.",
                f"{s.count} {s.kind.replace('_', ' ')} windows from {s.window_start}")
    for r in _immediate(scope):
        i = r["insp"] or {}
        add(r["tag"], "Inspection", "High", 14, f"Follow-up inspection of {r['tag']}: "
            f"{i.get('recommended_action') or 'confirm wall thickness'}.",
            f"wall loss {_f(r['wl'])} %, band {r['risk_band']}, report {i.get('report_no') or 'n/a'}")
    for s in pre.signals:
        if s.kind in ("breach", "zscore"):
            add(s.asset_tag, "Monitor", "Low", 30, f"Review the {s.measurement} trend of {s.sensor_tag} with operations.",
                s.line())
    head = ["_Rule-based plan: Cloudera AI Inference unavailable._", "",
            f"{len(tickets)} draft tickets for {scope.facility_name} on {scope.business_date}, for engineer approval."]
    return "\n".join(head + [""] + (tickets or ["NO_TICKETS"]))


STAGES = {"condition_analysis": condition, "integrity_assessment": integrity, "operations_plan": plan}
