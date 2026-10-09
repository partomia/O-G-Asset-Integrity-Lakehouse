"""
CAI Model ogx-integrity: function predict() serves the corrosion champion
(models/champion, installed by job ogx-04).

Request (JSON), one of:
  {"image_b64": "<base64 JPEG/PNG>"}            a drone keyframe
  {"sha256": "<frame sha256>"}                 a frame of the committed library (assets/frames)
Response: {"severity", "probabilities": {none, surface, severe}, "band", "threshold", "model": {...}}
band: P1 severe probability >= p1_probability, P2 >= operating threshold, P3 otherwise.
"""
from __future__ import annotations

import base64
import csv
import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import joblib  # noqa: E402

from features.feature_logic import feature_hash, features, load_image  # noqa: E402

CHAMPION = ROOT / os.environ.get("OGX_CHAMPION_DIR", "models/champion")
_META = json.loads((CHAMPION / "model_meta.json").read_text())
_MODEL = joblib.load(CHAMPION / "model.joblib")
if _META["feature_hash"] != feature_hash():
    raise RuntimeError("champion was trained on different feature logic")
_FRAMES = {}
_csv = ROOT / "assets" / "frames" / "frames.csv"
if _csv.exists():
    with open(_csv) as f:
        _FRAMES = {r["sha256"]: ROOT / "assets" / "frames" / r["frame"] for r in csv.DictReader(f)}


def score(img) -> dict:
    proba = _MODEL.predict_proba(features(img).reshape(1, -1))[0]
    p = {str(c): float(v) for c, v in zip(_MODEL.classes_, proba)}
    sev = p.get("severe", 0.0)
    band = "P1" if sev >= _META["p1_probability"] else "P2" if sev >= _META["threshold"] else "P3"
    return {"severity": max(p, key=p.get), "probabilities": p, "band": band, "threshold": _META["threshold"],
            "model": {"name": _META["model"], "estimator": _META["estimator"], "git_sha": _META["git_sha"][:7],
                      "feature_version": _META["feature_version"],
                      "test_auroc": round(_META["metrics"]["test"]["auroc"], 3)}}


def predict(args: dict) -> dict:
    if "image_b64" in args:
        return score(load_image(base64.b64decode(args["image_b64"])))
    if "sha256" in args:
        path = _FRAMES.get(args["sha256"])
        if path is None or not path.exists():
            return {"error": f"unknown frame {args['sha256']}"}
        return {"sha256": args["sha256"], **score(load_image(path))}
    return {"error": "send image_b64 or sha256"}
