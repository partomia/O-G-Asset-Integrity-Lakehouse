"""
Drone frames unfit for AI review, made from real ones: the positives of the frame-quality
model (models.frame_qc) and the planted bad frames of the synthetic drone videos.

Each kind is a reason an inspector would re-fly rather than judge the frame:
  blur        motion blur from wind or a fast pass: blur of 1.2-2.2 % of the frame width
  low_light   dusk or shadow: dark, compressed toward black
  glare       sun on wet steel: burnt out toward white
  occlusion   rain drop or debris on the lens: 35-55 % of the frame covered by a soft blob
  noise       high-ISO sensor noise

Deterministic in (frame, seed): degrade(img, kind, seed, key) always returns the same frame.
The model only ever learns from these synthetic failures; a real deployment would retrain on
the frames its inspectors actually rejected, and says so.
"""
from __future__ import annotations

import hashlib
import random

import numpy as np
from PIL import Image, ImageDraw, ImageFilter

KINDS = ("blur", "low_light", "glare", "occlusion", "noise")


def _rng(key: str, seed: int) -> random.Random:
    return random.Random(int(hashlib.sha1(f"{seed}:{key}".encode()).hexdigest()[:12], 16))


def kind_for(key: str, seed: int) -> str:
    return _rng(f"kind:{key}", seed).choice(KINDS)


def degrade(img: Image.Image, kind: str, seed: int = 0, key: str = "") -> Image.Image:
    """An RGB frame with one failure of the given kind."""
    rng = _rng(f"{kind}:{key}", seed)
    img = img.convert("RGB")
    w, h = img.size
    if kind == "blur":
        return img.filter(ImageFilter.GaussianBlur(w * rng.uniform(0.012, 0.022)))
    if kind in ("low_light", "glare", "noise"):
        a = np.asarray(img, dtype=np.float32) / 255.0
        if kind == "low_light":
            a = (a ** rng.uniform(2.4, 3.2)) * rng.uniform(0.25, 0.4)
        elif kind == "glare":
            a = 1.0 - ((1.0 - a) ** rng.uniform(2.6, 3.6)) * rng.uniform(0.25, 0.45)
        else:
            a = a + np.random.default_rng(rng.randrange(2**31)).normal(0, rng.uniform(0.16, 0.24), a.shape)
        return Image.fromarray((np.clip(a, 0, 1) * 255).astype(np.uint8), "RGB")
    if kind == "occlusion":
        mask = Image.new("L", (w, h), 0)
        r = int(min(w, h) * rng.uniform(0.35, 0.5))
        cx, cy = rng.randint(r // 2, w - r // 2), rng.randint(r // 2, h - r // 2)
        ImageDraw.Draw(mask).ellipse((cx - r, cy - r, cx + r, cy + r), fill=255)
        mask = mask.filter(ImageFilter.GaussianBlur(r * 0.25))
        blob = img.filter(ImageFilter.GaussianBlur(w * 0.05)).point(lambda v: int(v * 0.55 + 60))
        return Image.composite(blob, img, mask)
    raise ValueError(f"unknown degradation {kind!r}; one of {KINDS}")
