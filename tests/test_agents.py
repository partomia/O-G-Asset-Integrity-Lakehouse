"""Agentic Integrity Monitor (agents/): pre-screen, ticket parser, LLM guardrails, runner and records, on fixtures."""
import json
from datetime import datetime, timedelta

import pytest

from agents import grounding, llm, prescreen, records, runner, tickets
from agents.context import Scope
from agents.llm import Completion, LLMError, Usage
from lakehouse import store

D = "2026-10-06"


def windows(sensor, asset, values, meas="pressure_psi", **flags):
    t0 = datetime(2026, 10, 6)
    return [{"sensor_tag": sensor, "asset_tag": asset, "measurement": meas, "window_start": t0 + timedelta(minutes=15 * i),
             "mean_value": v, "max_value": v + 1, **{k: (f(i) if callable(f) else f) for k, f in flags.items()}}
            for i, v in enumerate(values)]


def jitter(base, n=60):
    return [base + (0.5 if i % 2 else -0.5) for i in range(n)]


# ------------------------------------------------------------------ pre-screen
def test_zscore_fires_on_a_step_and_ignores_jitter():
    step = jitter(100.0)
    step[40] = 130.0
    pre = prescreen.scan(windows("PI-101A.PV", "P-101A", step) + windows("PI-102A.PV", "P-102A", jitter(100.0)))
    z = [s for s in pre.signals if s.kind == "zscore"]
    assert [s.sensor_tag for s in z] == ["PI-101A.PV"] and z[0].zscore > 3


def test_small_relative_change_is_not_an_anomaly():
    step = jitter(100.0)
    step[40] = 101.5                                    # z > 3 but 1.5 % off the mean
    assert not prescreen.scan(windows("PI-101A.PV", "P-101A", step), min_rel_change=0.10).signals


def test_daily_flags_come_from_gold_not_the_windows():
    w = windows("PI-W01.PV", "W-01", jitter(1800.0), is_breach=True)      # stale silver threshold
    w += windows("VI-112B.PV", "P-112B", [2.0] * 60, meas="vibration_mm_s", is_stuck=lambda i: i >= 10)
    daily = [{"tag": "W-01", "sensor_tag": "PI-W01.PV", "measurement": "pressure_psi", "breach_windows": 0},
             {"tag": "P-112B", "sensor_tag": "VI-112B.PV", "measurement": "vibration_mm_s", "stuck_windows": 50},
             {"tag": "P-105A", "sensor_tag": "PI-105A.PV", "measurement": "pressure_psi", "bar_readings": 1680,
              "max_value": 206.2}]
    pre = prescreen.scan(w, daily=daily)
    kinds = {(s.sensor_tag, s.kind) for s in pre.signals}
    assert kinds == {("VI-112B.PV", "stuck"), ("PI-105A.PV", "unit_change")}
    stuck = next(s for s in pre.signals if s.kind == "stuck")
    assert stuck.count == 50 and stuck.window_start.startswith("2026-10-06 02:30")
    assert "P-112B" in pre.to_context_block() and pre.flagged_assets[0] == "P-112B"


def test_nothing_flagged_says_so():
    pre = prescreen.scan(windows("PI-101A.PV", "P-101A", jitter(100.0)))
    assert not pre.has_signals and pre.to_context_block().startswith("No anomalies")


# ------------------------------------------------------------------ tickets
TICKETS_MD = """Summary.

```ticket
asset_tag: PL-03-SEG-027
ticket_type: inspection
priority: HIGH
due_in_days: 7 days
description: UT re-survey of the thinned area.
evidence: wall loss 38.7 %, IR-20261006-001
```
```ticket
asset_tag: P-112B
ticket_type: Sensor
priority: urgent
due_in_days: 900
description: Replace VI-112B transmitter.
```
```ticket
asset_tag: P-101A
ticket_type: Shutdown
description: no such type
```
"""


def test_ticket_parser_normalises_and_drops_bad_blocks():
    t = tickets.parse(TICKETS_MD)
    assert [(x.asset_tag, x.ticket_type, x.priority, x.due_in_days) for x in t] == [
        ("PL-03-SEG-027", "Inspection", "High", 7), ("P-112B", "Sensor", "Medium", 180)]
    assert tickets.parse("NO_TICKETS") == [] and tickets.summary_without_tickets(TICKETS_MD) == "Summary."


# ------------------------------------------------------------------ LLM guardrails
POLICY = grounding.policy(runner.ROOT / "config" / "guardrails.yaml")


def test_unknown_tag_and_ungrounded_numbers_fire():
    rep = grounding.Report()
    ctx = "PL-03-SEG-027 wall_loss_pct 38.7 exposure 2.35 breach 96"
    out = "PL-03-SEG-027 lost 38.7 % (exposure 2.4); P-999Z vibrates at 12.5, 44.1 and 71.3 mm/s, 96 windows."
    grounding.check_stage(rep, "condition_analysis", out, ctx, {"PL-03-SEG-027"}, POLICY)
    fired = {e["guardrail"]: e for e in rep.events}
    assert "P-999Z" in fired["llm_unknown_tag"]["reason"]
    assert fired["llm_ungrounded_number"]["value"] == pytest.approx(0.5)      # 3 of 6 checked numbers


def test_grounded_stage_is_clean_and_dates_are_not_numbers():
    rep = grounding.Report()
    ctx = "W-07 wall loss 6.4 exposure 0.501 on 2026-10-08 10:13"
    grounding.check_stage(rep, "s", "W-07: 6.4 % wall loss, exposure 0.50, inspected 2026-10-08 10:13.", ctx,
                          {"W-07"}, POLICY)
    assert rep.events == []


def test_citations_required_and_checked():
    rep = grounding.Report()
    files = {"IR_20261006_PL-03-SEG-027_01.pdf"}
    grounding.check_stage(rep, "integrity_assessment", "Pitting on PL-03-SEG-027.", "", {"PL-03-SEG-027"}, POLICY, files)
    grounding.check_stage(rep, "integrity_assessment", "See [IR_X.pdf p.1] and [IR_20261006_PL-03-SEG-027_01.pdf p.1].",
                          "", set(), POLICY, files)
    reasons = [e["reason"] for e in rep.events if e["guardrail"] == "llm_citation"]
    assert len(reasons) == 2 and "no [file p.N]" in reasons[0] and "IR_X.pdf" in reasons[1]


def test_ticket_guardrails_scope_abstained_forbidden():
    rep = grounding.Report()
    t = [tickets.TicketDraft("W-07", "Inspection", "High", 7, "Shut in the well and inspect."),
         tickets.TicketDraft("P-112B", "Repair", "Medium", 7, "Replace the bearing."),
         tickets.TicketDraft("V-999", "Monitor", "Low", 30, "Watch it."),
         tickets.TicketDraft("P-105A", "Sensor", "Medium", 7, "Recalibrate PI-105A.")]
    grounding.check_tickets(rep, t, {"W-07", "P-112B", "P-105A"}, {"P-112B"}, POLICY)
    assert [x.asset_tag for x in rep.kept] == ["W-07", "P-112B", "P-105A"]
    assert [x.asset_tag for x in rep.dropped] == ["V-999"]
    assert {e["guardrail"] for e in rep.events} == {"llm_forbidden_action", "llm_ticket_abstained", "llm_ticket_scope"}
    assert rep.kept[0].flags and rep.kept[1].flags and not rep.kept[2].flags


# ------------------------------------------------------------------ runner
WORKLIST = [
    {"risk_rank": 1, "tag": "PL-03-SEG-027", "asset_id": "OGX-1", "asset_class": "PIPE_SEGMENT", "criticality": "A",
     "risk_band": "HIGH", "wall_loss_pct": 38.7, "exposure": 2.35, "p_event_30d": 0.78, "is_abstained": 0},
    {"risk_rank": 2, "tag": "PL-03-SEG-041", "asset_id": "OGX-2", "asset_class": "PIPE_SEGMENT", "criticality": "A",
     "risk_band": "MEDIUM", "wall_loss_pct": None, "exposure": 0.87, "p_event_30d": 0.29, "is_abstained": 0},
]
DAILY = [{"tag": "PL-03-SEG-027", "asset_id": "OGX-1", "sensor_tag": "CP-PL03-027.PV", "measurement": "wall_loss_mm_y",
          "breach_windows": 96, "max_value": 0.95, "mean_value": 0.91}]
INSP = [{"asset_id": "OGX-1", "doc_id": "d1", "report_no": "IR-20261006-001", "file_name": "IR_20261006_PL-03-SEG-027_01.pdf",
         "inspected_at": datetime(2026, 10, 6, 9), "wall_loss_pct": 38.7, "corrosion_type": "PITTING", "cui": "YES",
         "recommended_action": "REPAIR OR REPLACE WITHIN 30 DAYS"}]
CHUNKS = [{"doc_id": "d1", "file_name": "IR_20261006_PL-03-SEG-027_01.pdf", "page_no": 1, "chunk_no": 0,
           "text": "WALL LOSS (%): 38.7 CORROSION TYPE: PITTING"}]


def fake_query(sql: str) -> list[dict]:
    if "mis_risk_worklist" in sql:
        return [dict(r) for r in WORKLIST]
    if "sensor_window" in sql:
        return windows("CP-PL03-027.PV", "PL-03-SEG-027", [0.91] * 30, meas="wall_loss_mm_y", is_breach=True)
    if "fact_sensor_daily s JOIN" in sql:
        return [dict(r) for r in DAILY]
    if "DISTINCT sensor_tag" in sql:
        return [{"sensor_tag": "CP-PL03-027.PV"}]
    if "dim_asset WHERE is_current" in sql:
        return [{"tag": r["tag"]} for r in WORKLIST]
    if "fact_inspection" in sql:
        return [dict(r) for r in INSP]
    if "fact_work_order" in sql:
        return []
    if "doc_chunk" in sql:
        return [dict(r) for r in CHUNKS]
    raise AssertionError(sql)


class FakeLLM:
    name, model, configured = "fake", "fake-model", True

    def __init__(self, outputs, fail_at=None):
        self.outputs, self.fail_at, self.calls = list(outputs), fail_at, []

    def chat_stream(self, messages, out, temperature=None, **_):
        self.calls.append(messages)
        if self.fail_at is not None and len(self.calls) - 1 >= self.fail_at:
            raise LLMError("CAII HTTP 503")
        text = self.outputs[len(self.calls) - 1]
        for i in range(0, len(text), 40):
            out.text += text[i:i + 40]
            yield text[i:i + 40]
        out.usage = Usage(100, 50, 150)


STAGE_OUT = [
    "PL-03-SEG-027 breaches 96 windows, wall loss 38.7 %.",
    "PL-03-SEG-027: pitting, 38.7 % wall loss [IR_20261006_PL-03-SEG-027_01.pdf p.1].",
    "Act on PL-03-SEG-027.\n" + TICKETS_MD.split("```ticket", 2)[0] + "```ticket" + TICKETS_MD.split("```ticket")[1]
    + "```ticket\nasset_tag: V-999\nticket_type: Monitor\npriority: Low\ndue_in_days: 30\ndescription: out of scope\n```",
]


def test_runner_streams_three_stages_and_keeps_scoped_tickets():
    fake = FakeLLM(STAGE_OUT)
    evs = list(runner.run(fake_query, "PL03", D, depth="deep", llm=fake))
    types = [e["type"] for e in evs]
    assert types[0] == "prescreen" and types[-1] == "done" and types.count("stage_completed") == 3
    assert "delta" in types
    # each stage sees the stages before it
    assert "Previous stage: Condition analysis" in fake.calls[1][1]["content"]
    assert "Previous stage: Integrity assessment" in fake.calls[2][1]["content"]
    assert "IR_20261006_PL-03-SEG-027_01.pdf p.1" in fake.calls[1][1]["content"]
    done = evs[-1]
    assert done["provider"] == "caii" and done["usage"].total_tokens == 450
    assert [t.asset_tag for t in done["tickets"]] == ["PL-03-SEG-027"]
    assert [t.asset_tag for t in done["dropped"]] == ["V-999"]
    assert any(e["guardrail"] == "llm_ticket_scope" for e in done["events"])


def test_quick_with_nothing_flagged_spends_no_tokens(monkeypatch):
    monkeypatch.setattr(runner.context, "enrich", lambda *a, **k: pytest.fail("no enrich on a short circuit"))

    def quiet(sql):
        if "sensor_window" in sql:
            return windows("CP-PL03-027.PV", "PL-03-SEG-027", jitter(0.9))
        if "fact_sensor_daily s JOIN" in sql:
            return []
        return fake_query(sql)

    fake = FakeLLM([])
    evs = list(runner.run(quiet, "PL03", D, depth="quick", llm=fake))
    assert [e["type"] for e in evs] == ["prescreen", "short_circuit", "done"] and fake.calls == []


def test_llm_failure_falls_back_to_rule_based_stages():
    fake = FakeLLM(STAGE_OUT, fail_at=1)
    done = list(runner.run(fake_query, "PL03", D, depth="deep", llm=fake))[-1]
    assert [s["provider"] for s in done["stages"]] == ["caii", "offline", "offline"]
    assert done["provider"] == "caii+offline" and len(fake.calls) == 2
    assert any(e["guardrail"] == "llm_unavailable" for e in done["events"])
    assert done["tickets"] and done["tickets"][0].asset_tag == "PL-03-SEG-027"
    assert "Rule-based" in done["stages"][2]["markdown"]


# ------------------------------------------------------------------ records
def test_records_match_their_tables():
    done = list(runner.run(fake_query, "PL03", D, depth="deep", llm=FakeLLM(STAGE_OUT)))[-1]
    rows = records.analysis_rows(done)
    for table, rs in rows.items():
        cols = {c for c, _ in store.TABLES[table]}
        for r in rs:
            assert set(r) <= cols, (table, set(r) - cols)
    t = rows["agent_ticket"][0]
    assert t["asset_id"] == "OGX-1" and t["status"] == "DRAFT" and str(t["due_date"]) == "2026-10-13"
    assert rows["agent_analysis"][0]["n_tickets"] == 1
    assert {g["layer"] for g in rows["guardrail_event"]} == {"output"}
    assert "ROW_NUMBER()" in records.tickets_sql("p")


# ------------------------------------------------------------------ CAII client
class FakeResp:
    def __init__(self, status, lines):
        self.status_code, self.lines, self.text = status, lines, "err"

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_lines(self, decode_unicode=True):
        return iter(self.lines)


def test_caii_stream_parses_sse_usage_and_caches(monkeypatch):
    calls = []
    lines = [f"data: {json.dumps({'choices': [{'delta': {'content': 'Hel'}}]})}", "",
             f"data: {json.dumps({'choices': [{'delta': {'content': 'lo'}}]})}",
             f"data: {json.dumps({'choices': [], 'usage': {'prompt_tokens': 5, 'completion_tokens': 2, 'total_tokens': 7}})}",
             "data: [DONE]"]
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: calls.append(k) or FakeResp(200, lines))
    c = llm.CAIIChat("https://caii/v1", "m", token="t")
    out = Completion()
    assert "".join(c.chat_stream([{"role": "user", "content": "hi"}], out)) == "Hello"
    assert out.usage.total_tokens == 7 and calls[0]["headers"]["Authorization"] == "Bearer t"
    again = Completion()
    assert "".join(c.chat_stream([{"role": "user", "content": "hi"}], again)) == "Hello" and again.cached
    assert len(calls) == 1


def test_caii_circuit_opens_after_failures(monkeypatch):
    monkeypatch.setattr(llm.requests, "post", lambda *a, **k: FakeResp(400, []))
    c = llm.CAIIChat("https://caii/v1", "m", token="t")
    for _ in range(c.breaker_failures):
        with pytest.raises(LLMError):
            list(c.chat_stream([{"role": "user", "content": "x"}], Completion(), use_cache=False))
    assert c.breaker_open
    with pytest.raises(LLMError, match="circuit open"):
        list(c.chat_stream([{"role": "user", "content": "x"}], Completion()))


def test_unconfigured_endpoint_runs_rule_based():
    done = list(runner.run(fake_query, "PL03", D, depth="deep", llm=llm.CAIIChat("", "", token="t")))[-1]
    assert done["provider"] == "offline" and done["tickets"]
    assert any(e["guardrail"] == "llm_unavailable" for e in done["events"])


def test_scope_blocks_carry_no_sensitive_columns():
    s = Scope("PL03", D, [dict(WORKLIST[0], latitude=1.0, frame_sha256="x")], [], set())
    s.focus = ["PL-03-SEG-027"]
    b = json.dumps(s.blocks())
    assert "latitude" not in b and "sha256" not in b
