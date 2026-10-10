"""
Model versions in the Cloudera AI Registry (copied from CXR's serve/registry.py, adapted): one
registered model per head (models.<name>.registry_name, e.g. ogx-corrosion), one version per
champion or silent trial.

A version is created from the candidate's MLflow run (train/train_validate.py logs the
scikit-learn model, its parameters and TEST metrics) when job ogx-04 deploys it, and again
when ogx-07 promotes a silent trial. The audit tags (stage, git commit, feature hash,
threshold, KPIs) are sent at creation, but this workbench stores none and cannot update them,
so the stage, the approver and the version number are also written to ref.model_event, which
the Workbench and the dashboards read.

Registering never fails a job: the model is already deployed; the registry records it.
"""
from __future__ import annotations

import os

KPIS = ("auroc", "sensitivity", "specificity", "brier")
LAST_ERROR = ""   # why the last registrar() call registered nothing, for the model event's detail


def _get(obj, key, default=None):
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def version_tags(meta: dict, stage: str, extra: dict | None = None) -> list[dict]:
    from lakehouse.store import model_version

    test = meta["metrics"]["test"]
    tags = {"stage": stage, "model": meta.get("model"), "model_version": model_version(meta),
            "estimator": meta.get("estimator"), "git_sha": meta["git_sha"][:7],
            "feature_version": meta["feature_version"], "feature_hash": meta["feature_hash"],
            "threshold": f"{meta['threshold']:.4f}", "train_rows": meta.get("train_rows"),
            "feedback_rows": meta.get("feedback_rows"),
            **{f"test_{k}": f"{float(test[k]):.4f}" for k in KPIS if test.get(k) is not None},
            **(extra or {})}
    return [{"key": k, "value": str(v)} for k, v in tags.items() if v is not None]


def find_model(client, name: str):
    models = _get(client.list_registered_models(page_size=100), "models") or []
    return next((m for m in models if _get(m, "name") == name), None)


def versions(client, name: str) -> list:
    model = find_model(client, name)
    if model is None:
        return []
    return _get(client.get_registered_model(_get(model, "model_id")), "model_versions") or []


def register_version(client, meta: dict, name: str, stage: str, experiment_id: str,
                     project_id: str | None = None, extra: dict | None = None, description: str = "") -> dict:
    """The candidate's MLflow model as a new version of `name`; {model_id, version, number, previous}."""
    before = versions(client, name)
    created = client.create_registered_model({
        "project_id": project_id or os.environ.get("CDSW_PROJECT_ID", ""), "experiment_id": experiment_id,
        "run_id": meta["mlflow_run_id"], "model_path": "model", "model_name": name,
        "tags": version_tags(meta, stage, extra), "description": description,
        "notes": f"{stage}: git {meta['git_sha'][:7]}, gate passed", "visibility": "PRIVATE"})
    model_id = _get(created, "model_id")
    after = _get(client.get_registered_model(model_id), "model_versions") or []
    new = max(after, key=lambda v: _get(v, "number") or 0)
    prev = max((_get(v, "number") or 0 for v in before), default=None)
    out = {"model_id": model_id, "version": _get(new, "model_version_id"), "number": _get(new, "number"),
           "previous": prev}
    print(f"registry: {name} version {out['number']} ({stage}; previous {prev})", flush=True)
    return out


def registrar(cfg: dict, meta: dict, stage: str, extra: dict | None = None) -> dict | None:
    """register_version from inside a CAI job (cmlapi.default_client). None when disabled or on error."""
    global LAST_ERROR
    LAST_ERROR = ""
    if not cfg.get("registry", {}).get("enabled") or not meta.get("mlflow_run_id"):
        LAST_ERROR = "registry disabled or no MLflow run"
        print(f"registry: {LAST_ERROR} - not registered")
        return None
    try:
        import cmlapi
        import mlflow

        # the CAI MLflow plugin cannot get_run without an experiment set: look the experiment up by name
        name = cfg["project"]["mlflow_experiment"]
        experiment_id = (mlflow.set_experiment(name) or mlflow.get_experiment_by_name(name)).experiment_id
        return register_version(cmlapi.default_client(), meta, cfg["model"]["registry_name"], stage,
                                experiment_id, extra=extra, description=cfg["model"].get("description", ""))
    except Exception as e:  # noqa: BLE001  the deployment stands; the registry is its record
        LAST_ERROR = f"{type(e).__name__}: {e}"[:500]
        print(f"registry: WARNING not registered: {LAST_ERROR}")
        return None
