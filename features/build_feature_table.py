"""
Job ogx-01-build-features: the corrosion feature table from data/raw/corrosion (job
ogx-setup-data), labelled none / surface / severe by features/labels.py.

Writes feature_store/ogx_features/<FEATURE_VERSION>/features.npz (X, y, split, files) and
meta.json (feature version and hash, class counts), which train and gate check.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402

from features.feature_logic import FEATURE_VERSION, feature_hash, features, load_image  # noqa: E402
from common import finish  # noqa: E402
from features.labels import severity_of  # noqa: E402

RAW = ROOT / "data" / "raw" / "corrosion"
OUT = ROOT / "feature_store" / "ogx_features" / FEATURE_VERSION


def main() -> int:
    t0 = time.time()
    X, y, split, files = [], [], [], []
    for sp in ("train", "valid", "test"):
        imgs = sorted((RAW / sp / "images").glob("*"))
        if not imgs:
            raise SystemExit(f"no images in {RAW / sp}: run job ogx-setup-data first")
        for p in imgs:
            lab = RAW / sp / "labels" / (p.stem + ".txt")
            y.append(severity_of(lab.read_text() if lab.exists() else ""))
            X.append(features(load_image(p)))
            split.append(sp)
            files.append(p.name)
    OUT.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(OUT / "features.npz", X=np.stack(X), y=np.array(y), split=np.array(split),
                        files=np.array(files))
    counts = {sp: {c: int(sum(1 for a, b in zip(split, y) if a == sp and b == c)) for c in ("none", "surface", "severe")}
              for sp in ("train", "valid", "test")}
    meta = {"feature_version": FEATURE_VERSION, "feature_hash": feature_hash(), "n": len(y),
            "dim": int(len(X[0])), "counts": counts, "seconds": round(time.time() - t0, 1)}
    (OUT / "meta.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta), flush=True)
    return 0


if __name__ == "__main__":
    finish(main())
