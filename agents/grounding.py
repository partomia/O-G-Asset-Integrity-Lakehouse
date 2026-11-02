"""
LLM guardrail (config/guardrails.yaml `llm`): the agents' text is checked against the context
they were given before anything reaches an engineer.

  llm_unknown_tag        an asset or sensor tag that does not exist in the asset master
  llm_ungrounded_number  numbers in a stage that appear nowhere in its context
  llm_citation           the integrity stage cites no inspection report page, or cites one it was not given
  llm_ticket_scope       a ticket for an asset outside the facility analysed: dropped
  llm_ticket_abstained   a non-Sensor ticket on an asset abstained for a stuck sensor: flagged
  llm_forbidden_action   a ticket that tells someone to act outside "rank, suggest": flagged

Every check that fires is an event (guardrails/events.py -> ref.guardrail_event).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml

from agents.tickets import TicketDraft

PREFIXES = ("PL", "W", "P", "K", "V", "E", "C", "TK", "FL", "XV", "PSV", "PI", "VI", "CP", "TI")
TAG_RE = re.compile(r"\b(?:PL-\d{2}-SEG-\d{3}|CP-PL\d{2}-\d{3}|(?:PSV|TK|FL|XV|PI|VI|TI|W|P|K|V|E|C)-W?\d{2,3}[A-Z]?)"
                    r"(?:\.PV)?\b")
NUM_RE = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?(?![\w])")
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?\b|\b\d{1,2}:\d{2}\b")
CITE_RE = re.compile(r"\[([^\]\s]+\.pdf)\s*p\.?\s*(\d+)\]", re.IGNORECASE)


@dataclass
class Report:
    events: list[dict] = field(default_factory=list)
    kept: list[TicketDraft] = field(default_factory=list)
    dropped: list[TicketDraft] = field(default_factory=list)

    def fire(self, guardrail: str, value, limit, reason: str) -> None:
        self.events.append({"guardrail": guardrail, "value": value, "limit": limit, "reason": reason})


def policy(path) -> dict:
    return (yaml.safe_load(open(path)) or {}).get("llm", {})


def tags_in(text: str) -> set[str]:
    return {t.removesuffix(".PV") for t in TAG_RE.findall(text or "")}


ID_RE = re.compile(r"\b[A-Za-z]{1,6}[-_][\w\-.]*\d[\w\-.]*")      # IR-20261006-001, IR_..._01.pdf, OGX-10000039


def numbers_in(text: str) -> list[float]:
    text = ID_RE.sub(" ", TAG_RE.sub(" ", DATE_RE.sub(" ", text or "")))
    text = re.sub(r"(?<=\d),(?=\d{3}\b)", "", text)
    out = []
    for n in NUM_RE.findall(text):
        try:
            out.append(float(n))
        except ValueError:
            pass
    return out


def _grounded(x: float, pool: list[float]) -> bool:
    return any(abs(x - c) <= max(0.06, 0.011 * abs(c)) for c in pool)


def ungrounded_numbers(output: str, context: str) -> tuple[list[float], int]:
    """Numbers worth checking (decimals, or integers >= 10) that the context does not contain.
    Integers below 10 are list numbers and counts too small to mean much either way."""
    pool = numbers_in(context)
    pool += [round(c, 1) for c in pool] + [round(c * 100, 1) for c in pool if abs(c) < 1]
    checked = [x for x in numbers_in(output) if x != int(x) or abs(x) >= 10]
    return [x for x in checked if not _grounded(x, pool)], len(checked)


def check_stage(rep: Report, stage: str, output: str, context: str, known_tags: set[str], cfg: dict,
                doc_files: set[str] | None = None) -> None:
    unknown = sorted(tags_in(output) - known_tags - tags_in(context))
    if unknown:
        rep.fire("llm_unknown_tag", len(unknown), "0", f"{stage}: not in the asset master: {', '.join(unknown[:8])}")
    bad, n = ungrounded_numbers(output, context)
    lim = cfg.get("max_ungrounded_number_share", 0.25)
    if n >= cfg.get("min_numbers_checked", 4) and len(bad) / n > lim:
        sample = ", ".join(f"{x:g}" for x in bad[:8])
        rep.fire("llm_ungrounded_number", round(len(bad) / n, 3), f"<= {lim}",
                 f"{stage}: {len(bad)} of {n} numbers not in the context ({sample})")
    if doc_files is not None and cfg.get("require_page_citations", True):
        cites = CITE_RE.findall(output or "")
        if doc_files and not cites:
            rep.fire("llm_citation", 0, ">= 1", f"{stage}: no [file p.N] citation although report excerpts were given")
        foreign = sorted({f for f, _ in cites if f not in doc_files})
        if foreign:
            rep.fire("llm_citation", len(foreign), "0", f"{stage}: cites reports it was not given: {', '.join(foreign[:5])}")


def check_tickets(rep: Report, tickets: list[TicketDraft], scope_tags: set[str], abstained: set[str],
                  cfg: dict) -> None:
    forbidden = [re.compile(p, re.IGNORECASE) for p in cfg.get("forbidden_actions", [])]
    for t in tickets:
        tag = t.asset_tag.removesuffix(".PV")
        if tag not in scope_tags:
            rep.fire("llm_ticket_scope", 1, "facility assets", f"ticket for {t.asset_tag} dropped: not an asset of this facility")
            rep.dropped.append(t)
            continue
        t.asset_tag = tag
        if tag in abstained and t.ticket_type != "Sensor":
            t.flags.append("asset abstained for a stuck sensor: confirm the instrument first")
            rep.fire("llm_ticket_abstained", 1, "ticket_type Sensor", f"{tag}: {t.ticket_type} ticket on an abstained asset")
        hits = [p.pattern for p in forbidden if p.search(f"{t.description} {t.evidence}")]
        if hits:
            t.flags.append("outside the allowed actions (rank, suggest): engineer to rewrite")
            rep.fire("llm_forbidden_action", len(hits), "rank, suggest_inspection", f"{tag}: matches {hits[0]}")
        rep.kept.append(t)
