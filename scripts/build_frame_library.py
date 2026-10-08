#!/usr/bin/env python3
"""
Build the committed drone frame library assets/frames/ from the Bi3Q3 TEST split.

The synthetic drone videos (generators/gen_videos.py, standard library only) are MJPEG AVIs
whose frames are these JPEG files byte for byte, so the keyframes the CDE extractor pulls out
have the same sha256 as a library frame, and CAI scoring finds the image by that hash.

  assets/frames/<sha256[:16]>.jpg   320 px wide, JPEG quality 80
  assets/frames/frames.csv          frame, sha256, source image, severity, corrosion area,
                                    boxes, degradation kind ('' for a real frame), licence

Only TEST images are used, so no video frame was ever seen in training. Each real frame also
gets one degraded copy (features/degrade.py), the planted unfit frames. Licence: CC-BY-4.0,
Roboflow 100 "Corrosion Bi3Q3" (assets/frames/ATTRIBUTION.md).

  python scripts/build_frame_library.py [--raw data/raw/corrosion]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

from features.degrade import degrade, kind_for  # noqa: E402
from features.labels import corrosion_stats, severity  # noqa: E402

WIDTH, QUALITY, SEED = 320, 80, 20261004
ATTRIBUTION = """# Drone frame library: attribution

Every real frame here is a resized copy of an image from the TEST split of
**Corrosion Bi3Q3** (Roboflow 100 benchmark, https://universe.roboflow.com/roboflow-100/corrosion-bi3q3),
mirrored on Hugging Face as `LibreYOLO/corrosion-bi3q3` at revision
`d20b5b1e6b21ff7de555bbf848dcf7d30ccf8ee7`, licensed **CC BY 4.0**
(https://creativecommons.org/licenses/by/4.0/).

Changes: resized to 320 px wide and re-encoded as JPEG; frames with a non-empty `degraded`
column in `frames.csv` were further altered (blur, low light, glare, occlusion or noise) by
`features/degrade.py`. Severity labels are derived from the dataset's boxes by
`features/labels.py`. Built by `scripts/build_frame_library.py`.
"""


def jpeg(img: Image.Image) -> bytes:
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=QUALITY, optimize=False, progressive=False)
    return buf.getvalue()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(ROOT / "data/raw/corrosion"))
    ap.add_argument("--out", default=str(ROOT / "assets/frames"))
    args = ap.parse_args()
    raw, out = Path(args.raw) / "test", Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.jpg"):
        old.unlink()
    rows = []
    for img_path in sorted((raw / "images").iterdir()):
        label = raw / "labels" / (img_path.stem + ".txt")
        area, n = corrosion_stats(label.read_text() if label.exists() else "")
        img = Image.open(img_path).convert("RGB")
        img = img.resize((WIDTH, max(1, round(img.height * WIDTH / img.width))), Image.BILINEAR)
        for kind in ("", kind_for(img_path.name, SEED)):
            data = jpeg(degrade(img, kind, SEED, img_path.name) if kind else img)
            sha = hashlib.sha256(data).hexdigest()
            name = f"{sha[:16]}.jpg"
            (out / name).write_bytes(data)
            rows.append({"frame": name, "sha256": sha, "source_image": img_path.name,
                         "severity": severity(area, n), "corrosion_area": round(area, 5), "boxes": n,
                         "degraded": kind, "licence": "CC-BY-4.0"})
    with open(out / "frames.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    (out / "ATTRIBUTION.md").write_text(ATTRIBUTION)
    real = [r for r in rows if not r["degraded"]]
    counts = {s: sum(r["severity"] == s for r in real) for s in ("none", "surface", "severe")}
    size = sum((out / r["frame"]).stat().st_size for r in rows)
    print(f"{len(rows)} frames ({len(real)} real, {counts}), {size / 1e6:.1f} MB -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
