"""
Job ogx-03-kpi-gate: the candidate's TEST metrics against config/pipeline.yaml gate. Exit code 1
on any miss, so the CAI dependency stops the chain before deploy. Writes gate_result.json next
to the candidate; deploy refuses a candidate without a passing result for the same git sha.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from common import finish, load_config  # noqa: E402
from features.feature_logic import feature_hash  # noqa: E402


def evaluate(meta: dict, gate: dict, champion: dict | None) -> list[tuple[str, float, str, float, bool]]:
    t = meta["metrics"]["test"]
    checks = [("auroc", t["auroc"], ">=", gate["min_auroc"], t["auroc"] >= gate["min_auroc"]),
              ("sensitivity", t["sensitivity"], ">=", gate["min_sensitivity"], t["sensitivity"] >= gate["min_sensitivity"]),
              ("specificity", t["specificity"], ">=", gate["min_specificity"], t["specificity"] >= gate["min_specificity"]),
              ("brier", t["brier"], "<=", gate["max_brier"], t["brier"] <= gate["max_brier"])]
    if gate.get("require_feature_hash_match", True):
        ok = meta["feature_hash"] == feature_hash()
        checks.append(("feature_hash_match", float(ok), "==", 1.0, ok))
    if champion and champion.get("feature_hash") == meta["feature_hash"]:
        floor = champion["metrics"]["test"]["auroc"] - gate["max_auroc_regression"]
        checks.append(("auroc_vs_champion", t["auroc"], ">=", floor, t["auroc"] >= floor))
    return checks


def main() -> int:
    cfg = load_config()
    cand = ROOT / cfg["serving"]["candidate_dir"]
    meta = json.loads((cand / "model_meta.json").read_text())
    champ_path = ROOT / cfg["serving"]["champion_dir"] / "model_meta.json"
    champion = json.loads(champ_path.read_text()) if champ_path.exists() else None
    checks = evaluate(meta, cfg["gate"], champion)
    for name, val, op, lim, ok in checks:
        print(f"{'PASS' if ok else 'FAIL'} {name}: {val:.3f} {op} {lim:.3f}", flush=True)
    passed = all(c[4] for c in checks)
    (cand / "gate_result.json").write_text(json.dumps(
        {"passed": passed, "candidate_git_sha": meta["git_sha"],
         "checks": [{"name": n, "value": v, "op": o, "limit": lim, "ok": ok} for n, v, o, lim, ok in checks]}, indent=2))
    print(f"gate {'PASSED' if passed else 'FAILED'} for {meta['model']} ({meta['estimator']})", flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    finish(main())
