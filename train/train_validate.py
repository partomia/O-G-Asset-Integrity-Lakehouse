"""
Job ogx-02-train-validate: the corrosion head on the feature table (job ogx-01).

Three tree-ensemble candidates are scored by 5-fold out-of-fold severe-vs-rest AUROC on
TRAIN + VALID; the best one's out-of-fold probabilities set the operating threshold for
evaluation.target_sensitivity (226 severe images rather than VALID's 64, so the threshold is
stable), it is refitted on TRAIN + VALID, and TEST is measured once. Out-of-fold figures are
optimistic (the Roboflow TRAIN split holds augmented copies); TEST is the honest number. Writes the candidate package to serving.candidate_dir: model.joblib and
model_meta.json (metrics, threshold, feature version and hash, git sha).
"""
from __future__ import annotations

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

import joblib  # noqa: E402
import numpy as np  # noqa: E402
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier  # noqa: E402
from sklearn.metrics import brier_score_loss, roc_auc_score  # noqa: E402
from sklearn.model_selection import StratifiedKFold, cross_val_predict  # noqa: E402

from common import finish, git_sha, load_config  # noqa: E402
from features.feature_logic import FEATURE_VERSION, feature_hash  # noqa: E402

CLASSES = ["none", "surface", "severe"]


def severe_prob(model, X) -> np.ndarray:
    return model.predict_proba(X)[:, list(model.classes_).index("severe")]


def threshold_for(y_pos: np.ndarray, p: np.ndarray, sensitivity: float) -> float:
    pos = np.sort(p[y_pos])
    if not len(pos):
        return 0.5
    k = int(np.floor((1 - sensitivity) * len(pos)))
    return float(pos[min(k, len(pos) - 1)])


def metrics(y_pos: np.ndarray, p: np.ndarray, thr: float) -> dict:
    pred = p >= thr
    tp, fn = int((pred & y_pos).sum()), int((~pred & y_pos).sum())
    tn, fp = int((~pred & ~y_pos).sum()), int((pred & ~y_pos).sum())
    return {"auroc": float(roc_auc_score(y_pos, p)), "sensitivity": tp / max(tp + fn, 1),
            "specificity": tn / max(tn + fp, 1), "brier": float(brier_score_loss(y_pos, p)),
            "tp": tp, "fn": fn, "tn": tn, "fp": fp, "n": int(len(p))}


def main() -> int:
    cfg = load_config()
    ft = ROOT / "feature_store" / "ogx_features" / FEATURE_VERSION
    fmeta = json.loads((ft / "meta.json").read_text())
    if fmeta["feature_hash"] != feature_hash():
        raise SystemExit("feature table was built with different feature logic: rerun ogx-01")
    d = np.load(ft / "features.npz")
    X, y, split = d["X"], d["y"], d["split"]
    tr, va, te = split == "train", split == "valid", split == "test"
    dev = ~te
    seed = int(cfg["data"]["split_seed"])
    candidates = {
        "random_forest": lambda: RandomForestClassifier(n_estimators=500, min_samples_leaf=5, n_jobs=1,
                                                        class_weight="balanced_subsample", random_state=seed),
        "extra_trees": lambda: ExtraTreesClassifier(n_estimators=500, min_samples_leaf=3, n_jobs=1,
                                                    class_weight="balanced_subsample", random_state=seed),
        "gradient_boosting": lambda: HistGradientBoostingClassifier(
            max_iter=150, learning_rate=0.05, max_leaf_nodes=8, min_samples_leaf=30, l2_regularization=5.0,
            class_weight="balanced", random_state=seed),
    }
    cv = StratifiedKFold(5, shuffle=True, random_state=seed)
    oof, scores = {}, {}
    for name, make in candidates.items():
        proba = cross_val_predict(make(), X[dev], y[dev], cv=cv, method="predict_proba")
        oof[name] = proba[:, sorted(set(y[dev])).index("severe")]
        scores[name] = float(roc_auc_score(y[dev] == "severe", oof[name]))
        print(f"{name}: out-of-fold severe AUROC {scores[name]:.3f}", flush=True)
    best = max(scores, key=scores.get)
    thr = threshold_for(y[dev] == "severe", oof[best], float(cfg["evaluation"]["target_sensitivity"]))
    model = candidates[best]().fit(X[dev], y[dev])
    out = {"valid": metrics(y[dev] == "severe", oof[best], thr),
           "test": metrics(y[te] == "severe", severe_prob(model, X[te]), thr)}
    acc3 = float((model.predict(X[te]) == y[te]).mean())
    cand = ROOT / cfg["serving"]["candidate_dir"]
    cand.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, cand / "model.joblib")
    meta = {"model": cfg["model"]["name"], "estimator": best, "classes": [str(c) for c in model.classes_],
            "threshold": thr, "p1_probability": float(cfg["evaluation"]["p1_probability"]),
            "feature_version": FEATURE_VERSION, "feature_hash": feature_hash(), "git_sha": git_sha(),
            "metrics": out, "test_accuracy_3class": acc3, "oof_auroc_by_estimator": scores,
            "trained_at": datetime.now(timezone.utc).isoformat()}
    (cand / "model_meta.json").write_text(json.dumps(meta, indent=2))
    t = out["test"]
    print(f"candidate {best}: threshold {thr:.3f}; TEST severe AUROC {t['auroc']:.3f}, sensitivity "
          f"{t['sensitivity']:.3f}, specificity {t['specificity']:.3f}, Brier {t['brier']:.3f}; 3-class accuracy {acc3:.3f}",
          flush=True)
    return 0


if __name__ == "__main__":
    finish(main())
