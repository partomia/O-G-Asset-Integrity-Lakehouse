"""
Job ogx-08-retrain-trigger (daily, after ogx-05-nightly-drift): decides whether the corrosion
champion is retrained, and if so starts the chain ogx-01 -> 02 -> 03 (KPI gate) -> 04 (deploy +
registry) through the Cloudera AI API v2. Nothing is deployed that does not pass the gate,
including non-regression against the serving champion.

Reasons (config/guardrails.yaml retrain), any one is enough:
  drift      outputs/monitoring/drift_report.json says ALERT (and on_drift_alert is true), and the
             champion is at least drift_cooldown_days old: retraining on unchanged data the
             night after a retrain would only reproduce the same model
  feedback   at least min_new_labels engineer frame labels since the champion was trained
  age        the champion is older than max_model_age_days
  forced     OGX_RETRAIN_FORCE=1 in the job run's environment (a person asked for it)
Records RETRAIN_TRIGGERED (with the reasons) or RETRAIN_SKIPPED in ref.model_event; the reason
travels to training as OGX_TRIGGER and ends up in the MLflow run and the model's events.

  python ci/retrain_trigger.py --dry-run     # decide and print, start nothing
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from common import finish, load_config, parse_args  # noqa: E402

FEEDBACK = ROOT / "outputs" / "feedback" / "frame_labels.csv"
DRIFT = ROOT / "outputs" / "monitoring" / "drift_report.json"
FIRST_JOB = "ogx-01-build-features"


def _ts(s: str) -> datetime:
    t = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def new_labels(since: datetime) -> int:
    if not FEEDBACK.exists():
        return 0
    with open(FEEDBACK) as f:
        return sum(1 for r in csv.DictReader(f) if r.get("ts_utc") and _ts(r["ts_utc"]) > since)


def decide(meta: dict | None, rules: dict, drift: dict | None, now: datetime, force: bool = False) -> list[str]:
    reasons = []
    if force:
        reasons.append("forced")
    if meta is None:
        return reasons + ["no champion"]
    trained = _ts(meta["trained_at"])
    age = (now - trained).days
    if rules.get("on_drift_alert") and drift and drift.get("status") == "ALERT" \
            and drift.get("model_version") in (None, meta.get("model_version")) \
            and age >= rules.get("drift_cooldown_days", 0):
        alerts = [k for k, v in drift.get("metrics", {}).items() if v.get("status") == "ALERT"]
        reasons.append(f"drift ({', '.join(alerts)})")
    n = new_labels(trained)
    if n >= rules["min_new_labels"]:
        reasons.append(f"feedback ({n} new engineer labels)")
    if age > rules["max_model_age_days"]:
        reasons.append(f"age ({age} days)")
    return reasons


def start_chain(reason: str) -> str:
    import cmlapi

    client = cmlapi.default_client()
    pid = os.environ["CDSW_PROJECT_ID"]
    jobs = client.list_jobs(pid, search_filter=json.dumps({"name": FIRST_JOB}), page_size=10).jobs
    job = next(j for j in jobs if j.name == FIRST_JOB)
    run = client.create_job_run(cmlapi.CreateJobRunRequest(
        project_id=pid, job_id=job.id, environment={"OGX_TRIGGER": reason[:200]}), pid, job.id)
    print(f"started {FIRST_JOB} run {run.id}: the chain continues to the gate and deploy", flush=True)
    return run.id


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = parse_args(ap)
    cfg = load_config()
    from guardrails.input_checks import config as guard_config
    from lakehouse.publish import publish_model_event

    rules = guard_config()["retrain"]
    champ = ROOT / cfg["serving"]["champion_dir"] / "model_meta.json"
    meta = json.loads(champ.read_text()) if champ.exists() else None
    drift = json.loads(DRIFT.read_text()) if DRIFT.exists() else None
    reasons = decide(meta, rules, drift, datetime.now(timezone.utc), os.environ.get("OGX_RETRAIN_FORCE") == "1")
    reason = "; ".join(reasons)
    print(f"champion {meta.get('model_version') if meta else None}; drift {drift and drift.get('status')}; "
          f"reasons: {reason or 'none'}", flush=True)
    if not reasons:
        publish_model_event("RETRAIN_SKIPPED", meta, stage="champion", detail="no drift alert, too few new labels, not stale")
        return 0
    if args.dry_run:
        print("dry run: would start the retraining chain")
        return 0
    run_id = start_chain(reason)
    publish_model_event("RETRAIN_TRIGGERED", meta, stage="champion", trigger_reason=reason,
                        detail=f"{FIRST_JOB} run {run_id}")
    return 0


if __name__ == "__main__":
    finish(main())
