"""
CAI Model ogx-integrity: function predict() serves the corrosion champion
(models/champion, installed by job ogx-04), behind the guardrails in guardrails/.

Request (JSON), one of:
  {"image_b64": "<base64 JPEG/PNG>"}            a drone keyframe
  {"sha256": "<frame sha256>"}                 a frame of the committed library (assets/frames)
  optional "criticality": "A" | "B" | "C"      the asset's criticality (safety floor)
Response: {"severity", "probabilities", "band", "band_reason", "threshold", "guardrails": [...],
           "guardrail_events": [...], "action", "allowed_actions", "model": {...}}
band: P1 severe probability >= p1_probability, P2 >= operating threshold, P3 otherwise;
      NA when an input guardrail fails (frame quality, out of distribution),
      UNCERTAIN within the abstain margin of the threshold; criticality A never below P2.
Errors come back as {"error": ...} with the exception, never as an opaque HTTP 400.
"""
from __future__ import annotations

import base64
import csv
import json
import os
import sys
import traceback
from pathlib import Path

# The replica gets 1 vCPU but OpenMP sees every host core: HistGradientBoosting then
# oversubscribes, is throttled and the endpoint answers "model busy". Must precede numpy.
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "1")


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

try:
    import cml.models_v1 as _cml_models

    cml_model = _cml_models.cml_model
except ImportError:  # outside CAI (tests, app, jobs)
    def cml_model(fn):
        return fn

CHAMPION = ROOT / os.environ.get("OGX_CHAMPION_DIR", "models/champion")
_META, _MODEL, _FRAMES, _LOAD_ERROR = {}, None, {}, None
# A load failure must not crash the replica (it would never turn ready and hide the cause):
# every call answers with the error instead, and nothing is scored.
try:
    import joblib

    from features.feature_logic import feature_hash, features, load_image
    from guardrails import input_checks, output_policy

    _META = json.loads((CHAMPION / "model_meta.json").read_text())
    _MODEL = joblib.load(CHAMPION / "model.joblib")
    if _META["feature_hash"] != feature_hash():   # process guardrail: feature hash lock
        raise RuntimeError("champion was trained on different feature logic")
    _csv = ROOT / "assets" / "frames" / "frames.csv"
    if _csv.exists():
        with open(_csv) as f:
            _FRAMES = {r["sha256"]: ROOT / "assets" / "frames" / r["frame"] for r in csv.DictReader(f)}
except Exception as e:  # noqa: BLE001
    _LOAD_ERROR = {"error": f"model not loaded: {type(e).__name__}: {e}", "root": str(ROOT),
                   "trace": traceback.format_exc()[-1500:]}


def model_info() -> dict:
    return {"name": _META["model"], "estimator": _META["estimator"], "git_sha": _META["git_sha"][:7],
            "feature_version": _META["feature_version"], "version": _META.get("model_version"),
            "registry_version": _META.get("registry_version"),
            "test_auroc": round(_META["metrics"]["test"]["auroc"], 3)}


def score(img, criticality: str | None = None) -> dict:
    checks = input_checks.frame_checks(img)
    x = features(img)
    checks.append(input_checks.ood_check(x, _META.get("ood")))
    p, sev = {}, None
    if all(c["passed"] for c in checks):
        proba = _MODEL.predict_proba(x.reshape(1, -1))[0]
        p = {str(c): float(v) for c, v in zip(_MODEL.classes_, proba)}
        sev = p.get("severe", 0.0)
    out = output_policy.apply(sev, float(_META["threshold"]), float(_META["p1_probability"]), checks, criticality)
    return {"severity": max(p, key=p.get) if p else None, "probabilities": p, "threshold": float(_META["threshold"]),
            "guardrails": checks, **out, "model": model_info()}


# A PBJ runtime's model server calls only a function wrapped by cml_model: without it every
# request hangs and the endpoint answers "model busy".
@cml_model
def predict(args: dict) -> dict:
    if _LOAD_ERROR:
        return _LOAD_ERROR
    try:
        if isinstance(args, str):
            args = json.loads(args)
        crit = args.get("criticality")
        if "image_b64" in args:
            return score(load_image(base64.b64decode(args["image_b64"])), crit)
        if "sha256" in args:
            path = _FRAMES.get(args["sha256"])
            if path is None or not path.exists():
                return {"error": f"unknown frame {args['sha256']}"}
            return {"sha256": args["sha256"], **score(load_image(path), crit)}
        return {"error": "send image_b64 or sha256"}
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-1500:]}
