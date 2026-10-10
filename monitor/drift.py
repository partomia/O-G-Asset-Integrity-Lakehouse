"""
Job ogx-05-nightly-drift: is the champion still looking at the data it was trained on?

  1. current set: the drone keyframes the lakehouse holds (silver.video_keyframe, through Impala;
     the committed frame library when the lakehouse is unreachable, e.g. on a laptop)
  2. every frame goes through the input guardrails and the champion (the same code path as the
     endpoint, serve/predict.py); each guardrail that fires is a row in ref.guardrail_event
  3. drift on the frames the model actually scored, against the training feature table:
       PSI of the severe probability, and the max / mean PSI over the 49 features
       out-of-distribution rate and NA (unfit frame) rate
     status OK / WARN / ALERT from config/guardrails.yaml process.drift; TOO_FEW below min_samples
  4. rows in ref.model_drift, outputs/monitoring/drift_report.json (read by ogx-08-retrain-trigger)
     and a DRIFT_ALERT model event when any metric alerts
"""
from __future__ import annotations

import csv
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402

from common import finish, load_config  # noqa: E402
from guardrails import events  # noqa: E402
from guardrails.input_checks import config as guard_config  # noqa: E402

REPORT = ROOT / "outputs" / "monitoring" / "drift_report.json"


def psi(ref: np.ndarray, cur: np.ndarray, bins: int = 10) -> float:
    """Population stability index with bins on the reference deciles."""
    ref, cur = np.asarray(ref, float), np.asarray(cur, float)
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / max(len(cur), 1)
    r, c = np.clip(r, 1e-4, None), np.clip(c, 1e-4, None)
    return float(((c - r) * np.log(c / r)).sum())


def status(value: float, warn: float, alert: float) -> str:
    return "ALERT" if value >= alert else "WARN" if value >= warn else "OK"


def current_frames() -> tuple[list[dict], str]:
    """[{sha256, subject}] for the keyframes in the lakehouse, else the frame library."""
    lib = {}
    with open(ROOT / "assets" / "frames" / "frames.csv") as f:
        for r in csv.DictReader(f):
            lib[r["sha256"]] = r
    if os.environ.get("OGX_IMPALA_USER"):
        try:
            from lakehouse.store import ImpalaStore

            s = ImpalaStore()
            rows = s.query(f"SELECT DISTINCT frame_sha256, asset_hint, video_id, frame_index FROM "
                           f"{s.t('silver.video_keyframe')}")
            out = [{"sha256": r["frame_sha256"], "subject": f"{r['asset_hint']}/{r['video_id']}#{r['frame_index']}"}
                   for r in rows if r["frame_sha256"] in lib]
            if out:
                return out, "lakehouse silver.video_keyframe"
        except Exception as e:  # noqa: BLE001
            print(f"lakehouse unreachable ({type(e).__name__}): using the frame library", flush=True)
    return [{"sha256": k, "subject": f"library/{v['frame']}"} for k, v in lib.items()], "frame library"


def main() -> int:
    load_config()
    g = guard_config()["process"]["drift"]
    import serve.predict as champion
    from features.feature_logic import FEATURE_VERSION, features, load_image
    from lakehouse.publish import publish_model_event
    from lakehouse.store import model_version, publish

    meta = champion._META
    version = model_version(meta)
    d = np.load(ROOT / "feature_store" / "ogx_features" / FEATURE_VERSION / "features.npz")
    ref_X = d["X"][d["split"] != "test"]
    ref_p = champion._MODEL.predict_proba(ref_X)[:, list(champion._MODEL.classes_).index("severe")]

    frames, source = current_frames()
    scored_X, scored_p, guard_rows, bands = [], [], [], {}
    for fr in frames:
        img = load_image(champion._FRAMES[fr["sha256"]])
        res = champion.score(img)
        bands[res["band"]] = bands.get(res["band"], 0) + 1
        guard_rows += events.rows(res["guardrail_events"], fr["subject"], version, "ogx-05-nightly-drift")
        if res["probabilities"]:
            scored_X.append(features(img))
            scored_p.append(res["probabilities"]["severe"])
    n, n_scored = len(frames), len(scored_p)
    ood = sum(1 for r in guard_rows if r["guardrail"] == "out_of_distribution")
    na = bands.get("NA", 0)
    run_id, now = uuid.uuid4().hex[:12], datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    metrics = {"ood_rate": (ood / max(n, 1), g["max_ood_rate"] / 2, g["max_ood_rate"]),
               "na_rate": (na / max(n, 1), g["max_na_rate"] / 2, g["max_na_rate"])}
    if n_scored >= g["min_samples"]:
        cur_X = np.stack(scored_X)
        fpsi = [psi(ref_X[:, j], cur_X[:, j]) for j in range(ref_X.shape[1])]
        metrics.update({"psi_score": (psi(ref_p, np.array(scored_p)), g["psi_warn"], g["psi_alert"]),
                        "psi_feature_max": (max(fpsi), g["psi_warn"] * 2, g["psi_alert"] * 2),
                        "psi_feature_mean": (float(np.mean(fpsi)), g["psi_warn"], g["psi_alert"])})
    rows = [{"run_id": run_id, "recorded_at": now, "model_version": version, "metric": k, "value": round(v, 4),
             "status": status(v, w, a), "n_reference": int(len(ref_X)), "n_current": n_scored,
             "detail": f"warn {w:.3f}, alert {a:.3f}; {source}"} for k, (v, w, a) in metrics.items()]
    if n_scored < g["min_samples"]:
        rows.append({"run_id": run_id, "recorded_at": now, "model_version": version, "metric": "psi_score",
                     "value": None, "status": "TOO_FEW", "n_reference": int(len(ref_X)), "n_current": n_scored,
                     "detail": f"fewer than {g['min_samples']} scored frames"})
    overall = "ALERT" if any(r["status"] == "ALERT" for r in rows) else \
        "WARN" if any(r["status"] == "WARN" for r in rows) else "OK"
    report = {"run_id": run_id, "recorded_at": now.isoformat(), "model_version": version, "source": source,
              "frames": n, "scored": n_scored, "bands": bands, "guardrail_events": len(guard_rows),
              "status": overall, "metrics": {r["metric"]: {"value": r["value"], "status": r["status"]} for r in rows}}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str), flush=True)
    events.log_local(guard_rows)
    publish("guardrail_event", guard_rows)
    publish("model_drift", rows)
    if overall == "ALERT":
        drift_rows = [{"guardrail": "drift_psi", "passed": False, "value": r["value"], "limit": r["detail"],
                       "reason": f"{r['metric']} alert"} for r in rows if r["status"] == "ALERT"]
        publish("guardrail_event", events.rows(drift_rows, f"model {version}", version, "ogx-05-nightly-drift"))
        publish_model_event("DRIFT_ALERT", meta, stage="champion",
                            detail=", ".join(f"{r['metric']}={r['value']}" for r in rows if r["status"] == "ALERT"))
    return 0


if __name__ == "__main__":
    finish(main())
