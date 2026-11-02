"""
Parse the ```ticket blocks of the operations stage (the AMP's ticket extractor, with an asset
tag in place of a well id). A block that does not parse is dropped, never guessed.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

_BLOCK = re.compile(r"```ticket\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_FIELD = re.compile(r"^\s*([A-Za-z_]+)\s*:\s*(.+?)\s*$", re.MULTILINE)
TYPES = ("Inspection", "Repair", "Sensor", "Monitor")
PRIORITIES = ("High", "Medium", "Low")


@dataclass
class TicketDraft:
    asset_tag: str
    ticket_type: str
    priority: str
    due_in_days: int
    description: str
    evidence: str = ""
    flags: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


def _pick(value: str, allowed: tuple[str, ...]) -> str | None:
    v = value.strip().strip("`\"'<>").lower()
    return next((a for a in allowed if a.lower() == v), None)


def parse(markdown: str) -> list[TicketDraft]:
    out: list[TicketDraft] = []
    for m in _BLOCK.finditer(markdown or ""):
        f = {k.lower(): v for k, v in _FIELD.findall(m.group(1))}
        tag = (f.get("asset_tag") or f.get("tag") or f.get("asset") or "").strip().strip("`<>")
        desc = (f.get("description") or "").strip()
        ttype = _pick(f.get("ticket_type") or f.get("type") or "", TYPES)
        if not tag or not desc or ttype is None:
            continue
        try:
            due = max(1, min(int(re.sub(r"[^0-9]", "", f.get("due_in_days", "")) or 14), 180))
        except ValueError:
            due = 14
        out.append(TicketDraft(tag, ttype, _pick(f.get("priority", ""), PRIORITIES) or "Medium", due, desc,
                               (f.get("evidence") or "").strip()))
    return out


def summary_without_tickets(markdown: str) -> str:
    return _BLOCK.sub("", markdown or "").strip()
