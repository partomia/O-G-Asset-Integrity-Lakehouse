"""
The monitor's records in <prefix>_ref (Iceberg, through lakehouse/store.py):

  agent_analysis        one row per analysis: scope, provider, tokens, latency, the full report
  agent_ticket          one row per draft ticket, status DRAFT
  agent_ticket_review   append-only engineer decisions (APPROVED / REJECTED); the latest wins
  guardrail_event       the LLM guardrails that fired (layer output / process)

Append-only: no UPDATE on Iceberg from the app; a ticket's status is its latest review.
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone

from guardrails import events as gev
from lakehouse import store

store.TABLES.update({
    "agent_analysis": [
        ("analysis_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("business_date", "DATE"), ("facility_id", "STRING"),
        ("scope_asset", "STRING"), ("depth", "STRING"), ("provider", "STRING"), ("model", "STRING"),
        ("prompt_tokens", "INT"), ("completion_tokens", "INT"), ("total_tokens", "INT"), ("latency_s", "DOUBLE"),
        ("n_signals", "INT"), ("n_tickets", "INT"), ("n_guardrail_events", "INT"), ("markdown", "STRING"),
        ("stages_json", "STRING")],
    "agent_ticket": [
        ("ticket_id", "STRING"), ("analysis_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("business_date", "DATE"),
        ("facility_id", "STRING"), ("asset_id", "STRING"), ("tag", "STRING"), ("ticket_type", "STRING"),
        ("priority", "STRING"), ("due_date", "DATE"), ("description", "STRING"), ("evidence", "STRING"),
        ("guardrail_flags", "STRING"), ("source", "STRING"), ("status", "STRING")],
    "agent_ticket_review": [
        ("review_id", "STRING"), ("ticket_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("decision", "STRING"),
        ("reviewer", "STRING"), ("note", "STRING")],
})


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def analysis_rows(done: dict) -> dict[str, list[dict]]:
    now = _now()
    bd = date.fromisoformat(done["business_date"])
    u = done["usage"]
    ids = done.get("asset_ids", {})
    a = {"analysis_id": done["analysis_id"], "recorded_at": now, "business_date": bd, "facility_id": done["facility"],
         "scope_asset": done.get("scope_asset"), "depth": done["depth"], "provider": done["provider"],
         "model": done.get("model") or "", "prompt_tokens": u.prompt_tokens, "completion_tokens": u.completion_tokens,
         "total_tokens": u.total_tokens, "latency_s": float(done["latency_s"]), "n_signals": done["n_signals"],
         "n_tickets": len(done["tickets"]), "n_guardrail_events": len(done["events"]), "markdown": done["markdown"],
         "stages_json": json.dumps([{k: s[k] for k in ("key", "title", "provider")} for s in done["stages"]])}
    source = "AI analysis" if "caii" in done["provider"] else "Rule-based analysis"
    t = [{"ticket_id": f"TK-{uuid.uuid4().hex[:10].upper()}", "analysis_id": done["analysis_id"], "recorded_at": now,
          "business_date": bd, "facility_id": done["facility"], "asset_id": ids.get(x.asset_tag), "tag": x.asset_tag,
          "ticket_type": x.ticket_type, "priority": x.priority, "due_date": bd + timedelta(days=x.due_in_days),
          "description": x.description, "evidence": x.evidence, "guardrail_flags": "; ".join(x.flags), "source": source,
          "status": "DRAFT"} for x in done["tickets"]]
    g = gev.rows(done["events"], subject=done["analysis_id"], model_version=done.get("model") or "",
                 source="agentic-monitor")
    return {"agent_analysis": [a], "agent_ticket": t, "guardrail_event": g}


def publish(done: dict, st: "store.ImpalaStore | None" = None) -> dict[str, int]:
    rows = analysis_rows(done)
    gev.log_local(rows["guardrail_event"])
    st = st or store.ImpalaStore()
    try:
        st.ensure("agent_ticket_review")      # the Work tickets view joins it before the first review
    except Exception as e:  # noqa: BLE001
        print(f"[lakehouse] WARNING agent_ticket_review not created: {str(e)[:200]}", flush=True)
    return {table: store.publish(table, rs, st) for table, rs in rows.items()}


def review(ticket_id: str, decision: str, reviewer: str, note: str, st: "store.ImpalaStore | None" = None) -> int:
    row = {"review_id": uuid.uuid4().hex[:16], "ticket_id": ticket_id, "recorded_at": _now(), "decision": decision,
           "reviewer": reviewer, "note": note}
    return store.publish("agent_ticket_review", [row], st)


def tickets_sql(prefix: str, where: str = "TRUE") -> str:
    """Tickets with their current status: the latest review, else DRAFT."""
    return (f"SELECT t.ticket_id, t.recorded_at, t.business_date, t.facility_id, t.tag, t.ticket_type, t.priority, "
            f"t.due_date, t.description, t.evidence, t.guardrail_flags, t.source, t.analysis_id, "
            f"COALESCE(r.decision, 'DRAFT') AS status, r.reviewer, r.note "
            f"FROM {prefix}_ref.agent_ticket t LEFT JOIN ("
            f"SELECT ticket_id, decision, reviewer, note, ROW_NUMBER() OVER (PARTITION BY ticket_id "
            f"ORDER BY recorded_at DESC) rn FROM {prefix}_ref.agent_ticket_review) r "
            f"ON r.ticket_id = t.ticket_id AND r.rn = 1 WHERE {where} "
            f"ORDER BY t.recorded_at DESC, CASE t.priority WHEN 'High' THEN 0 WHEN 'Medium' THEN 1 ELSE 2 END")
