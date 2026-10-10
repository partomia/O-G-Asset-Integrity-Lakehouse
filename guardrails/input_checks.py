"""
Input guardrails, run before a keyframe or an asset is scored (config/guardrails.yaml: input).

  frame_checks(img)            size, brightness, contrast, sharpness, glare -> band NA when any fails
  ood_stats(X) / ood_check()   standardised distance to the training features -> band NA, reason OOD
  asset_checks(asset)          unresolved asset or stuck sensor -> not scored / abstained

Every check returns {"guardrail", "passed", "value", "limit", "reason"}; nothing raises, so the
caller decides what a failure means and logs it (guardrails/events.py).
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def config() -> dict:
    import yaml

    return yaml.safe_load((ROOT / "config" / "guardrails.yaml").read_text())


def _check(name, passed, value, limit, reason) -> dict:
    return {"guardrail": name, "passed": bool(passed), "value": None if value is None else round(float(value), 4),
            "limit": limit, "reason": None if passed else reason}


def frame_checks(img, cfg: dict | None = None) -> list[dict]:
    c = (cfg or config())["input"]
    w, h = img.size
    small = img.convert("RGB").resize((160, 160))
    hsv = np.asarray(small.convert("HSV"), dtype=np.float32) / 255.0
    gray = np.asarray(small.convert("L"), dtype=np.float32) / 255.0
    v, s = hsv[..., 2], hsv[..., 1]
    sharp = float(np.concatenate([np.abs(np.diff(gray, axis=1)).ravel(), np.abs(np.diff(gray, axis=0)).ravel()]).mean())
    glare = float(((v > 0.97) & (s < 0.10)).mean())
    b = float(v.mean())
    return [
        _check("frame_size", min(w, h) >= c["min_side_px"], min(w, h), f">= {c['min_side_px']} px", "frame too small"),
        _check("brightness", c["brightness"]["min"] <= b <= c["brightness"]["max"], b,
               f"{c['brightness']['min']}..{c['brightness']['max']}", "too dark" if b < c["brightness"]["min"] else "overexposed"),
        _check("contrast", gray.std() >= c["min_contrast"], gray.std(), f">= {c['min_contrast']}", "no contrast (flat frame)"),
        _check("sharpness", sharp >= c["min_sharpness"], sharp, f">= {c['min_sharpness']}", "blurred"),
        _check("glare", glare <= c["max_glare_fraction"], glare, f"<= {c['max_glare_fraction']}", "glare"),
    ]


def ood_stats(X: np.ndarray, quantile: float | None = None) -> dict:
    """Training-set reference for ood_check, stored in model_meta.json by train_validate.py."""
    q = quantile or config()["input"]["ood_quantile"]
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd = np.where(sd < 1e-6, 1e-6, sd)
    d = (((X - mu) / sd) ** 2).mean(axis=1)
    return {"mean": mu.round(6).tolist(), "std": sd.round(6).tolist(), "limit": float(np.quantile(d, q)),
            "quantile": q, "train_median": float(np.median(d))}


def ood_distance(x: np.ndarray, stats: dict) -> float:
    mu, sd = np.asarray(stats["mean"]), np.asarray(stats["std"])
    return float((((np.asarray(x) - mu) / sd) ** 2).mean())


def ood_check(x: np.ndarray, stats: dict | None) -> dict:
    if not stats:
        return _check("out_of_distribution", True, None, "no reference", None)
    d = ood_distance(x, stats)
    return _check("out_of_distribution", d <= stats["limit"], d, f"<= {stats['limit']:.2f} "
                  f"(training q{stats['quantile']})", "unlike anything in training (OOD)")


def asset_checks(asset: dict, cfg: dict | None = None) -> list[dict]:
    c = (cfg or config())["input"]
    out = []
    if c.get("require_resolved_asset", True):
        ok = bool(asset.get("asset_id"))
        out.append(_check("asset_resolved", ok, None, "asset master match", "asset not resolved: not scored"))
    stuck = int(asset.get("stuck_windows_3d") or 0)
    out.append(_check("sensor_sanity", stuck < c["stuck_windows_abstain"], stuck, f"< {c['stuck_windows_abstain']} stuck windows",
                      "stuck sensor: asset abstains"))
    return out
