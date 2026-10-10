"""
Model events: one row per step of a model's life, written to outputs/model_events.jsonl in the
CAI project and to <prefix>_ref.model_event in the lakehouse (the Workbench's Models tab and the
OGX Models dashboard read the table; the registry itself stores no tags on this workbench).

Events: TRAINED, GATE_PASSED, GATE_FAILED, REGISTERED, DEPLOYED, ROLLED_BACK, RETRAIN_TRIGGERED,
RETRAIN_SKIPPED, PROMOTED, PROMOTION_REFUSED, DRIFT_ALERT.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.store import model_version, publish

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "outputs" / "model_events.jsonl"


def model_event_row(event: str, meta: dict | None = None, gate: dict | None = None, **kw) -> dict:
    meta = meta or {}
    t = (meta.get("metrics") or {}).get("test") or {}
    row = {"event_id": uuid.uuid4().hex[:16], "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0),
           "model": meta.get("model", kw.pop("model", "corrosion")), "event": event,
           "model_version": model_version(meta) if meta.get("feature_version") else None,
           "estimator": meta.get("estimator"), "git_sha": (meta.get("git_sha") or "")[:7] or None,
           "feature_version": meta.get("feature_version"), "feature_hash": meta.get("feature_hash"),
           "threshold": meta.get("threshold"), "test_auroc": t.get("auroc"), "test_sensitivity": t.get("sensitivity"),
           "test_specificity": t.get("specificity"), "test_brier": t.get("brier"), "train_rows": meta.get("train_rows"),
           "feedback_rows": meta.get("feedback_rows"), "mlflow_run_id": meta.get("mlflow_run_id"),
           "registry_name": meta.get("registry_name"), "registry_version": meta.get("registry_version"),
           "gate_passed": gate.get("passed") if gate else None,
           "gate_checks": json.dumps(gate.get("checks"), default=str) if gate else None}
    row.update(kw)
    if row.get("detail"):
        row["detail"] = str(row["detail"])[:2000]
    return row


def publish_model_event(event: str, meta: dict | None = None, gate: dict | None = None, **kw) -> dict:
    row = model_event_row(event, meta, gate, **kw)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a") as f:
        f.write(json.dumps(row, default=str) + "\n")
    print(f"[model-event] {event} {row.get('model_version') or ''} {row.get('detail') or ''}"[:300], flush=True)
    publish("model_event", [row])
    return row


def last_event(event: str, model: str = "corrosion") -> dict | None:
    """The newest local event of a kind (the retrain trigger compares against the last TRAINED)."""
    if not LOG.exists():
        return None
    hits = [json.loads(line) for line in LOG.read_text().splitlines() if line.strip()]
    hits = [h for h in hits if h.get("event") == event and h.get("model") == model]
    return hits[-1] if hits else None
