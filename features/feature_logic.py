"""
Image features for the corrosion head, shared by training (features/build_feature_table.py)
and serving (serve/predict.py) so both compute exactly the same vector.

Colour and texture statistics, numpy and Pillow only, so a 1 vCPU CAI workload builds the
feature table in about a minute and the model build installs no deep-learning stack:
  - HSV histograms (hue weighted by saturation, saturation, value)
  - rust mask: share of pixels with an orange-to-brown hue and enough saturation, overall and
    the maximum over a 4 x 4 grid (a local patch of severe corrosion)
  - per-channel RGB mean and standard deviation
  - edge density and gradient statistics (pitting and flaking are rough)
The ViT backbone in config/pipeline.yaml is the planned upgrade; FEATURE_VERSION and the hash
of this file guard the feature/model pairing either way.
"""
from __future__ import annotations

import hashlib
import io
from pathlib import Path

import numpy as np
from PIL import Image

FEATURE_VERSION = "hc-1.0.0"
SIZE = 160
GRID = 4


def feature_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes() + FEATURE_VERSION.encode()).hexdigest()[:16]


def load_image(src) -> Image.Image:
    if isinstance(src, (bytes, bytearray)):
        return Image.open(io.BytesIO(src)).convert("RGB")
    return Image.open(src).convert("RGB")


def features(img: Image.Image) -> np.ndarray:
    img = img.convert("RGB").resize((SIZE, SIZE))
    rgb = np.asarray(img, dtype=np.float32) / 255.0
    hsv = np.asarray(img.convert("HSV"), dtype=np.float32) / 255.0
    h, s, v = hsv[..., 0], hsv[..., 1], hsv[..., 2]

    h_hist = np.histogram(h, bins=18, range=(0, 1), weights=s)[0]
    h_hist = h_hist / max(h_hist.sum(), 1e-6)
    s_hist = np.histogram(s, bins=8, range=(0, 1))[0] / s.size
    v_hist = np.histogram(v, bins=8, range=(0, 1))[0] / v.size

    rust = ((h <= 0.11) | (h >= 0.97)) & (s >= 0.30) & (v >= 0.15) & (v <= 0.85)
    cells = rust.reshape(GRID, SIZE // GRID, GRID, SIZE // GRID).mean(axis=(1, 3))
    rust_f = [rust.mean(), cells.max(), (cells > 0.10).mean(), (cells > 0.30).mean()]

    moments = np.concatenate([rgb.mean(axis=(0, 1)), rgb.std(axis=(0, 1))])

    gray = rgb.mean(axis=2)
    gx, gy = np.abs(np.diff(gray, axis=1)), np.abs(np.diff(gray, axis=0))
    grad = np.concatenate([gx.ravel(), gy.ravel()])
    rough = [grad.mean(), grad.std(), (grad > 0.08).mean(), (grad > 0.20).mean()]
    rust_rough = [float((gx[rust[:, 1:]] > 0.08).mean()) if rust[:, 1:].any() else 0.0]

    return np.concatenate([h_hist, s_hist, v_hist, rust_f, moments, rough, rust_rough]).astype(np.float32)
