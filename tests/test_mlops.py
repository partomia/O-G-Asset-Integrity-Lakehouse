"""MLOps plumbing: retrain decision, drift PSI, model versions, registry tags, lakehouse literals,
the CAI job graph and the NiFi flow configuration."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RULES = {"on_drift_alert": True, "min_new_labels": 20, "max_model_age_days": 30, "drift_cooldown_days": 1}
NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)
META = {"model": "corrosion", "estimator": "gradient_boosting", "feature_version": "hc-1.0.0",
        "feature_hash": "abc", "git_sha": "1234567890", "threshold": 0.3, "trained_at": "2026-10-09T03:00:00+00:00",
        "model_version": "gradient_boosting-hc-1.0.0-1234567-202610090300", "mlflow_run_id": "run-1",
        "metrics": {"test": {"auroc": 0.86, "sensitivity": 0.93, "specificity": 0.51, "brier": 0.12}}}


def test_retrain_reasons(monkeypatch, tmp_path):
    from ci import retrain_trigger as rt

    monkeypatch.setattr(rt, "FEEDBACK", tmp_path / "none.csv")
    assert rt.decide(META, RULES, None, NOW) == []
    assert rt.decide(META, RULES, None, NOW, force=True) == ["forced"]
    assert rt.decide(None, RULES, None, NOW) == ["no champion"]
    drift = {"status": "ALERT", "model_version": META["model_version"], "metrics": {"psi_score": {"status": "ALERT"}}}
    assert rt.decide(META, RULES, drift, NOW) == ["drift (psi_score)"]
    stale = {**drift, "model_version": "an-older-champion"}   # an alert about a model no longer serving
    assert rt.decide(META, RULES, stale, NOW) == []
    assert rt.decide(META, {**RULES, "drift_cooldown_days": 7}, drift, NOW) == []   # retrained 2 days ago
    assert rt.decide(META, RULES, None, NOW + timedelta(days=40))[0].startswith("age")


def test_retrain_on_feedback(monkeypatch, tmp_path):
    from ci import retrain_trigger as rt

    fb = tmp_path / "frame_labels.csv"
    lines = ["ts_utc,sha256,label"] + [f"2026-10-10T0{i % 9}:00:00+00:00,s{i},severe" for i in range(25)]
    fb.write_text("\n".join(lines) + "\n")
    monkeypatch.setattr(rt, "FEEDBACK", fb)
    assert rt.decide(META, RULES, None, NOW) == ["feedback (25 new engineer labels)"]


def test_psi():
    from monitor.drift import psi, status

    rng = np.random.default_rng(0)
    ref = rng.normal(0, 1, 2000)
    assert psi(ref, rng.normal(0, 1, 2000)) < 0.05
    assert psi(ref, rng.normal(1.5, 1, 2000)) > 0.25
    assert status(0.3, 0.1, 0.25) == "ALERT" and status(0.12, 0.1, 0.25) == "WARN" and status(0.01, 0.1, 0.25) == "OK"


def test_model_version_and_registry_tags():
    from lakehouse.store import model_version
    from serve.registry import version_tags

    assert model_version({k: v for k, v in META.items() if k != "model_version"}) == \
        "gradient_boosting-hc-1.0.0-1234567-202610090300"
    tags = {t["key"]: t["value"] for t in version_tags(META, "champion", {"approved_by": "x"})}
    assert tags["stage"] == "champion" and tags["test_auroc"] == "0.8600" and tags["approved_by"] == "x"


def test_registrar_never_fails_a_job():
    from serve import registry

    assert registry.registrar({"registry": {"enabled": False}}, META, "champion") is None
    assert registry.registrar({"registry": {"enabled": True}}, {**META, "mlflow_run_id": None}, "champion") is None


def test_lakehouse_literals():
    from lakehouse.store import TABLES, lit

    assert lit(None) == "NULL" and lit(True) == "TRUE" and lit(3) == "3" and lit(0.5) == "0.5"
    assert lit("it's") == "'it\\'s'"
    assert lit(datetime(2026, 10, 11, 1, 2, 3)).startswith("CAST('2026-10-11 01:02:03'")
    assert {"model_event", "guardrail_event", "model_drift"} <= set(TABLES)


def test_model_event_row_without_lakehouse(monkeypatch, tmp_path):
    from lakehouse import publish

    monkeypatch.delenv("OGX_IMPALA_USER", raising=False)
    monkeypatch.setattr(publish, "LOG", tmp_path / "events.jsonl")
    row = publish.publish_model_event("TRAINED", META, stage="candidate", trigger_reason="test")
    assert row["model_version"] == META["model_version"] and row["test_auroc"] == 0.86
    assert publish.last_event("TRAINED")["trigger_reason"] == "test"


def test_cai_job_graph():
    from ci.cai_jobs import CHAIN, JOBS

    names = [j["name"] for j in JOBS]
    assert CHAIN == ["ogx-00-sync-code", "ogx-01-build-features", "ogx-02-train-validate", "ogx-03-kpi-gate",
                     "ogx-04-deploy-champion"]
    by = {j["name"]: j for j in JOBS}
    assert by["ogx-05-nightly-drift"]["schedule"] and by["ogx-08-retrain-trigger"]["schedule"]
    for j in JOBS:   # parents first, and a job has a parent or a schedule, never both
        assert not (j["parent"] and j["schedule"])
        if j["parent"]:
            assert names.index(j["parent"]) < names.index(j["name"])
    for n in ("ogx-05-nightly-drift", "ogx-08-retrain-trigger", *CHAIN):
        assert (ROOT / by[n]["script"]).exists()


def test_nifi_output_topic_is_not_ingested_by_the_bronze_stream():
    cfg = json.loads((ROOT / "config" / "streaming.json").read_text())
    assert cfg["nifi"]["out_topic"]["name"] not in {t["name"] for t in cfg["topics"].values()}
    assert cfg["nifi"]["out_topic"]["name"].startswith("ogx.")
    assert "rsingh_ogx/" in cfg["nifi"]["landing_dir"] and "rsingh_ogx/" in cfg["nifi"]["quarantine_dir"]


def test_nifi_flow_export_has_no_secrets():
    flow = ROOT / "nifi" / "flows" / "ogx-scada-alarm-router.json"
    if not flow.exists():
        return
    sensitive = ("sasl.password", "basic-auth-password", "Kerberos Password")
    found = []

    def walk(o):
        if isinstance(o, dict):
            props = o.get("properties")
            if isinstance(props, dict):
                found.extend(k for k in sensitive if props.get(k))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(json.loads(flow.read_text()))
    assert not found, f"sensitive properties exported with a value: {found}"
