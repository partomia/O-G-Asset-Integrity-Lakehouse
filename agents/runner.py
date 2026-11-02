"""
Three agents in sequence (the AMP's runner): condition analyst -> integrity engineer ->
operations planner. Each stage's prompt carries the reports of the stages before it, streams
token by token, and is checked by the LLM guardrail (agents/grounding.py). The last stage's
ticket blocks become draft tickets for an engineer.

`run` yields events (dicts with a "type"); the app renders them, tests collect them:
  prescreen, short_circuit, stage_started, delta, stage_completed, guardrail, tickets, done
If the LLM fails, that stage and the ones after it fall back to agents/offline.py.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import yaml

from agents import context, grounding, offline, prescreen, tickets
from agents.llm import CFG, CAIIChat, Completion, LLMError, Usage

ROOT = Path(__file__).resolve().parents[1]
AGENTS = yaml.safe_load((ROOT / "agents" / "config" / "agents.yaml").read_text())
TASKS = yaml.safe_load((ROOT / "agents" / "config" / "tasks.yaml").read_text())
ORDER = ("condition_analysis", "integrity_assessment", "operations_plan")
POLICY = grounding.policy(ROOT / "config" / "guardrails.yaml")


def system_prompt(task_key: str) -> str:
    a = AGENTS[TASKS[task_key]["agent"]]
    return (f"You are the {a['role']}.\nGoal: {a['goal']}\nBackstory: {a['backstory'].strip()}\n"
            "Write Markdown. Cite exact asset tags and numbers from the context. Never invent tags, readings, "
            "reports or work orders that are not in the context; say 'not in the data' instead.\n"
            f"Expected output: {TASKS[task_key]['expected_output'].strip()}")


def fill(template: str, ctx: dict[str, str]) -> str:
    out = template
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def new_id() -> str:
    return f"AN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"


def run(query: context.Query, facility: str, business_date: str, asset_tag: str | None = None,
        depth: str = "quick", llm: CAIIChat | None = None) -> Iterator[dict]:
    t0 = time.monotonic()
    aid = new_id()
    llm = llm if llm is not None else CAIIChat()
    pcfg = CFG["prescreen"]
    scope = context.load(query, facility, business_date, asset_tag)
    pre = prescreen.scan(scope.windows, pcfg["rolling_windows"], pcfg["z_threshold"], pcfg["min_rel_change"],
                         daily=scope.daily)
    yield {"type": "prescreen", "analysis_id": aid, "block": pre.to_context_block(pcfg["top"]),
           "by_kind": pre.by_kind(), "flagged": pre.flagged_assets, "windows": pre.windows_scanned,
           "assets": len(scope.worklist)}

    base = {"analysis_id": aid, "facility": facility, "business_date": business_date, "scope_asset": asset_tag,
            "depth": depth, "n_signals": len(pre.signals), "model": llm.model}
    if depth == "quick" and not pre.has_signals:
        md = f"All clear: {pre.to_context_block()}"
        yield {"type": "short_circuit", "markdown": md}
        yield {"type": "done", **base, "provider": "prescreen", "markdown": md, "stages": [], "tickets": [],
               "dropped": [], "events": [], "usage": Usage(), "latency_s": round(time.monotonic() - t0, 1)}
        return

    context.enrich(query, scope, pre.flagged_assets)
    ctx = {**scope.blocks(), "anomaly_block": pre.to_context_block(pcfg["top"]),
           "max_tickets": str(POLICY.get("max_tickets", 8))}
    rep = grounding.Report()
    usage = Usage()
    stages: list[dict] = []
    provider = "caii" if llm.configured else "offline"
    if provider == "offline":
        rep.fire("llm_unavailable", 0, "endpoint configured", "CAII endpoint not configured: rule-based stages")

    for i, key in enumerate(ORDER):
        task = TASKS[key]
        sid, title = f"stage-{i + 1}-{key}", task["title"]
        user = fill(task["description"], ctx)
        prior = "\n\n".join(f"### Previous stage: {s['title']}\n\n{s['markdown']}" for s in stages)
        if prior:
            user = f"{prior}\n\n---\n\n{user}"
        yield {"type": "stage_started", "stage_id": sid, "title": title, "agent": AGENTS[task["agent"]]["role"],
               "provider": provider}
        text, stage_provider = "", provider
        if provider == "caii":
            out = Completion()
            try:
                for delta in llm.chat_stream([{"role": "system", "content": system_prompt(key)},
                                              {"role": "user", "content": user}], out,
                                             temperature=AGENTS[task["agent"]].get("temperature")):
                    yield {"type": "delta", "stage_id": sid, "text": delta}
                text = out.text.strip()
                usage.add(out.usage)
            except LLMError as e:
                provider = stage_provider = "offline"
                rep.fire("llm_unavailable", 1, "completion", f"{key}: {str(e)[:200]}; rule-based from here")
                yield {"type": "delta", "stage_id": sid, "text": f"\n\n_LLM failed ({str(e)[:120]}); rule-based stage._\n\n"}
        if stage_provider == "offline" or not text:
            text = offline.STAGES[key](scope, pre)
            stage_provider = "offline"
            yield {"type": "delta", "stage_id": sid, "text": text}
        n_before = len(rep.events)
        if stage_provider == "caii":
            grounding.check_stage(rep, key, text, f"{user}\n{system_prompt(key)}", scope.known_tags, POLICY,
                                  scope.doc_files if key == "integrity_assessment" else None)
        for ev in rep.events[n_before:]:
            yield {"type": "guardrail", "stage_id": sid, **ev}
        stages.append({"stage_id": sid, "key": key, "title": title, "markdown": text, "provider": stage_provider})
        yield {"type": "stage_completed", "stage_id": sid, "markdown": text, "provider": stage_provider}

    drafts = tickets.parse(stages[-1]["markdown"])
    n_before = len(rep.events)
    grounding.check_tickets(rep, drafts, scope.scope_tags, scope.abstained, POLICY)
    for ev in rep.events[n_before:]:
        yield {"type": "guardrail", "stage_id": "tickets", **ev}
    yield {"type": "tickets", "tickets": [t.as_dict() for t in rep.kept], "dropped": [t.as_dict() for t in rep.dropped]}
    md = "\n\n".join(f"## {s['title']}\n\n{s['markdown']}" for s in stages)
    used = {s["provider"] for s in stages}
    yield {"type": "done", **base, "provider": "+".join(sorted(used)), "markdown": md, "stages": stages,
           "tickets": rep.kept, "dropped": rep.dropped, "events": rep.events, "usage": usage,
           "asset_ids": {t: scope.asset_by_tag[t]["asset_id"] for t in scope.scope_tags},
           "latency_s": round(time.monotonic() - t0, 1)}
