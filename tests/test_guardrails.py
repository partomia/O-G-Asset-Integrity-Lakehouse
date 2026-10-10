"""Every guardrail in config/guardrails.yaml fires on cue (PLAN: 'each with a test that shows it firing')."""
from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
from PIL import Image

from features.degrade import degrade
from guardrails import events, input_checks, output_policy

ROOT = Path(__file__).resolve().parents[1]
FRAMES = ROOT / "assets" / "frames"


def clean_frames(n=None):
    with open(FRAMES / "frames.csv") as f:
        rows = [r for r in csv.DictReader(f) if not r["degraded"]]
    return [Image.open(FRAMES / r["frame"]).convert("RGB") for r in rows[:n] if r]


def fired(checks):
    return {c["guardrail"] for c in checks if not c["passed"]}


def test_clean_frames_pass_the_frame_checks():
    frames = clean_frames()
    rejected = sum(bool(fired(input_checks.frame_checks(img))) for img in frames)
    assert rejected / len(frames) <= 0.03, f"{rejected} of {len(frames)} clean frames rejected"


def test_glare_and_low_light_fire():
    img = clean_frames(1)[0]
    assert fired(input_checks.frame_checks(degrade(img, "glare", 0, "t"))) & {"brightness", "glare", "contrast", "sharpness"}
    assert "brightness" in fired(input_checks.frame_checks(degrade(img, "low_light", 0, "t")))


def test_blur_fires_sharpness():
    img = Image.fromarray((np.random.default_rng(0).random((200, 200, 3)) * 255).astype("uint8"))
    from PIL import ImageFilter

    assert "sharpness" in fired(input_checks.frame_checks(img.filter(ImageFilter.GaussianBlur(12))))


def test_tiny_frame_fires_size():
    assert "frame_size" in fired(input_checks.frame_checks(Image.new("RGB", (32, 32), (120, 90, 60))))


def test_ood_check_fires_far_from_training():
    X = np.random.default_rng(1).normal(0, 1, (500, 10))
    stats = input_checks.ood_stats(X, 0.995)
    assert input_checks.ood_check(X[0], stats)["passed"]
    assert not input_checks.ood_check(np.full(10, 8.0), stats)["passed"]
    assert input_checks.ood_check(np.zeros(10), None)["passed"]   # no reference: never blocks


def test_asset_checks_unresolved_and_stuck():
    assert fired(input_checks.asset_checks({"asset_id": None})) == {"asset_resolved"}
    assert fired(input_checks.asset_checks({"asset_id": "A1", "stuck_windows_3d": 2})) == {"sensor_sanity"}
    assert not fired(input_checks.asset_checks({"asset_id": "A1", "stuck_windows_3d": 0}))


def test_output_bands_and_abstain():
    ok = [{"guardrail": "brightness", "passed": True, "reason": None}]
    assert output_policy.apply(0.9, 0.3, 0.8, ok)["band"] == "P1"
    assert output_policy.apply(0.5, 0.3, 0.8, ok)["band"] == "P2"
    assert output_policy.apply(0.1, 0.3, 0.8, ok)["band"] == "P3"
    unsure = output_policy.apply(0.32, 0.3, 0.8, ok)
    assert unsure["band"] == "UNCERTAIN" and unsure["guardrail_events"][0]["guardrail"] == "abstain_band"


def test_failed_input_gives_na():
    bad = [{"guardrail": "glare", "passed": False, "reason": "glare"}]
    out = output_policy.apply(None, 0.3, 0.8, bad)
    assert out["band"] == "NA" and out["band_reason"] == "glare"


def test_safety_floor_raises_criticality_a():
    ok = [{"guardrail": "brightness", "passed": True, "reason": None}]
    out = output_policy.apply(0.05, 0.3, 0.8, ok, criticality="A")
    assert out["band"] == "P2" and any(e["guardrail"] == "safety_floor" for e in out["guardrail_events"])
    assert output_policy.apply(0.05, 0.3, 0.8, ok, criticality="C")["band"] == "P3"


def test_no_automated_action():
    out = output_policy.apply(0.95, 0.3, 0.8, [])
    assert out["action"] == "engineer review required"
    assert not any("work_order" in a for a in out["allowed_actions"])


def test_event_rows_carry_layer_and_subject():
    rows = events.rows([{"guardrail": "glare", "passed": False, "value": 0.7, "limit": "<= 0.5", "reason": "glare"},
                        {"guardrail": "abstain_band", "passed": False, "value": 0.31, "limit": "x", "reason": "u"}],
                       "TK-504/V1#3", "gb-hc-1.0.0-abc", "test")
    assert [r["layer"] for r in rows] == ["input", "output"]
    assert rows[0]["subject"] == "TK-504/V1#3" and rows[0]["model_version"] == "gb-hc-1.0.0-abc"


def test_endpoint_applies_guardrails_when_a_champion_exists():
    import pytest

    if not (ROOT / "models" / "champion" / "model.joblib").exists():
        pytest.skip("no local champion")
    import serve.predict as p

    sha = next(iter(p._FRAMES))
    out = p.predict({"sha256": sha, "criticality": "A"})
    assert "error" not in out, out
    assert out["band"] in output_policy.ORDER and out["guardrails"]
    assert out["action"] == "engineer review required"
    assert "error" in p.predict({"nothing": 1})
