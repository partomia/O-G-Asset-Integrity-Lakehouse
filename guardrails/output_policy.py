"""
Output guardrails (config/guardrails.yaml: output), applied to every model score before anyone
sees it, by the endpoint (serve/predict.py) and the Workbench alike:

  1. failed input guardrail      -> band NA (reviewed in calendar order, as without AI)
  2. abstain band                -> band UNCERTAIN when the severe probability is within
                                    uncertain_margin of the operating threshold
  3. safety-critical floor       -> a criticality-A asset never drops below P2 on a score alone
  4. no automated action         -> the response carries only allowed_actions; an engineer decides
"""
from __future__ import annotations

from guardrails.input_checks import config

ORDER = ["P1", "P2", "UNCERTAIN", "P3", "NA"]   # worklist order, most urgent first


def raw_band(p_severe: float, threshold: float, p1: float) -> str:
    return "P1" if p_severe >= p1 else "P2" if p_severe >= threshold else "P3"


def apply(p_severe: float | None, threshold: float, p1: float, input_results: list[dict],
          criticality: str | None = None, cfg: dict | None = None) -> dict:
    c = (cfg or config())["output"]
    fired = [r for r in input_results if not r["passed"]]
    events = list(fired)
    if fired or p_severe is None:
        band, reason = "NA", "; ".join(r["reason"] for r in fired) or "no score"
    else:
        band, reason = raw_band(p_severe, threshold, p1), None
        if band != "P1" and abs(p_severe - threshold) < c["uncertain_margin"]:
            events.append({"guardrail": "abstain_band", "passed": False, "value": round(p_severe, 4),
                           "limit": f"|p - {threshold:.3f}| >= {c['uncertain_margin']}",
                           "reason": "uncertain, engineer review"})
            band, reason = "UNCERTAIN", "uncertain, engineer review"
    floor = c.get("safety_floor") or {}
    if criticality in floor.get("criticality", []) and band in ("P3", "UNCERTAIN"):
        events.append({"guardrail": "safety_floor", "passed": False, "value": None,
                       "limit": f"criticality {criticality} -> at least {floor['min_band']}",
                       "reason": f"safety-critical asset raised from {band} to {floor['min_band']}"})
        reason = f"safety floor (was {band})"
        band = floor["min_band"]
    return {"band": band, "band_reason": reason, "allowed_actions": list(c["allowed_actions"]),
            "action": "engineer review required", "guardrail_events": events}
