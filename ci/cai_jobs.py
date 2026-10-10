"""
The CAI jobs of this project: one definition read by ci/setup_cai.py (creates them) and
ci/trigger_cai_pipeline.py (runs and follows them). Pure Python, so the GitHub runner can
import it without cmlapi. Copied from CXR and adapted.

Each model has a CHAIN of build-features, train-validate, kpi-gate, deploy, run in that order
through CAI job dependencies; a dependent job starts only when its parent succeeds, which
makes the KPI gate's exit code a hard stop. The job's environment OGX_MODEL picks the model:
  ogx-*       corrosion, champion: ranks the inspection worklist; ogx-00 syncs the code first
  ogx-qc-*    frame_qc, champion: an unfit frame gets band NA
  ogx-risk-*  equipment_risk, silent trial: scored daily in the lakehouse, shown to nobody,
              until ogx-07-promote-champion (manual, named approver) finds go_live met
The federal quota fits one 2 vCPU workload beside the endpoint: chains run one after another.
ogx-setup-data runs once; ogx-stream-producer backfills or streams live; ogx-06 is started by
the lakehouse DAG (cde/dags/ogx_dag.py) for one business date at a time.
MLOps loop without a push: ogx-05-nightly-drift (02:00) scores the lakehouse keyframes through
the guardrails and measures drift; ogx-08-retrain-trigger (02:30) starts ogx-01 when drift
alerts, enough engineer labels arrived or the champion is stale (ci/retrain_trigger.py).
"""
from __future__ import annotations

import json
import os

RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"


def _chain(prefix: str, model: str, parent: str | None) -> list[dict]:
    env = {"OGX_MODEL": model}
    steps = [("01-build-features", "features/build_feature_table.py", 1, 4, 7200),
             ("02-train-validate", "train/train_validate.py", 1, 4, 3600),
             ("03-kpi-gate", "gate/kpi_gate.py", 1, 2, 600),
             ("04-deploy", "serve/deploy_champion.py", 1, 2, 5400)]
    out = []
    for step, script, cpu, mem, timeout in steps:
        name = f"{prefix}-{step}" + ("-champion" if step == "04-deploy" else "")
        out.append({"name": name, "script": script, "parent": parent, "cpu": cpu, "memory": mem,
                    "timeout": timeout, "schedule": None, "env": env})
        parent = name
    return out


JOBS = [
    {"name": "ogx-setup-data", "script": "scripts/fetch_dataset.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 7200, "schedule": None},
    {"name": "ogx-stream-producer", "script": "stream/producer/sensor_producer.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 0, "schedule": None,
     "env": {"OGX_PRODUCER_SINK": "kafka", "OGX_PRODUCER_SETUP": "1"}},
    {"name": "ogx-00-sync-code", "script": "ci/sync_code.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 3600, "schedule": None},
    *_chain("ogx", "corrosion", "ogx-00-sync-code"),
    # ogx-qc-* (frame_qc) and ogx-risk-* (equipment_risk) join once their feature layouts exist:
    # *_chain("ogx-qc", "frame_qc", None), *_chain("ogx-risk", "equipment_risk", None),
    {"name": "ogx-05-nightly-drift", "script": "monitor/drift.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 3600, "schedule": "0 2 * * *"},
    {"name": "ogx-06-score-inspections", "script": "lakehouse/score_inspections.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 3600, "schedule": None},
    {"name": "ogx-07-promote-champion", "script": "serve/promote_champion.py", "parent": None,
     "cpu": 1, "memory": 2, "timeout": 5400, "schedule": None},
    # MLOps loop: drift and guardrail rates at 02:00, the retrain decision at 02:30 (it starts ogx-01)
    {"name": "ogx-08-retrain-trigger", "script": "ci/retrain_trigger.py", "parent": None,
     "cpu": 1, "memory": 2, "timeout": 600, "schedule": "30 2 * * *"},
]

CHAINS = {
    "corrosion": ["ogx-00-sync-code"] + [j["name"] for j in JOBS
                                         if j["name"].startswith("ogx-0") and j.get("env", {}).get("OGX_MODEL")],
    "frame_qc": [j["name"] for j in JOBS if j["name"].startswith("ogx-qc-")],
    "equipment_risk": [j["name"] for j in JOBS if j["name"].startswith("ogx-risk-")],
}
CHAIN = CHAINS["corrosion"]
GATE_JOBS = {n for c in CHAINS.values() for n in c if "-03-kpi-gate" in n}
GATE_JOB = "ogx-03-kpi-gate"
SCORE_JOB = "ogx-06-score-inspections"
PROMOTE_JOB = "ogx-07-promote-champion"


def resolve_runtime(client, configured: str = "") -> str:
    """The runtime image for jobs and model builds: the configured one, else the runtime of the
    current session/job (matched on the ML_RUNTIME_* variables CAI sets)."""
    if configured:
        return configured
    wanted = {k: os.environ.get(f"ML_RUNTIME_{k.upper()}") for k in ("kernel", "edition", "editor")}
    if not all(wanted.values()):
        return RUNTIME
    found, token = [], None
    while True:   # the API pages its results: the session's runtime can be past the first page
        kw = {"page_token": token} if token else {}
        resp = client.list_runtimes(search_filter=json.dumps(wanted), page_size=100, **kw)
        found += list(resp.runtimes or [])
        token = getattr(resp, "next_page_token", None)
        if not token:
            break
    full = os.environ.get("ML_RUNTIME_FULL_VERSION")
    exact = [r for r in found if full and getattr(r, "full_version", None) == full]
    pick = (exact or sorted(found, key=lambda r: getattr(r, "full_version", "") or ""))
    return pick[-1].image_identifier if pick else RUNTIME
