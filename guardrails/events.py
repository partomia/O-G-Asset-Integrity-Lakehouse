"""
Guardrail events: one row per guardrail that fired, kept locally (outputs/guardrails/events.jsonl
in the CAI project) and published to the lakehouse table ref.guardrail_event by the jobs that
score in batch (monitor/drift.py). The endpoint returns its events in the response instead:
a model replica's disk does not outlive it.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "outputs" / "guardrails" / "events.jsonl"

LAYER = {"frame_size": "input", "brightness": "input", "contrast": "input", "sharpness": "input", "glare": "input",
         "out_of_distribution": "input", "asset_resolved": "input", "sensor_sanity": "input",
         "abstain_band": "output", "safety_floor": "output", "kpi_gate": "process", "feature_hash_lock": "process",
         "drift_psi": "process", "named_approver": "process"}


def rows(events: list[dict], subject: str, model_version: str = "", source: str = "") -> list[dict]:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    return [{"event_id": uuid.uuid4().hex[:16], "recorded_at": now, "layer": LAYER.get(e["guardrail"], "input"),
             "guardrail": e["guardrail"], "subject": subject, "value": e.get("value"), "limit_rule": str(e.get("limit")),
             "reason": e.get("reason"), "model_version": model_version, "source": source} for e in events]


def log_local(rs: list[dict], path: Path = LOG) -> None:
    if not rs:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        for r in rs:
            f.write(json.dumps(r, default=str) + "\n")
