"""
Job ogx-setup-data: the pinned public corrosion image set, into data/raw/corrosion/.

  Roboflow 100 "Corrosion Bi3Q3", mirrored on Hugging Face as LibreYOLO/corrosion-bi3q3,
  pinned at revision d20b5b1e6b21ff7de555bbf848dcf7d30ccf8ee7. Licence CC-BY-4.0
  (attribution: Roboflow 100 benchmark, https://github.com/roboflow/roboflow-100-benchmark).
  1,249 images (train 840 / valid 304 / test 105, about 67 MB) with YOLO boxes for the
  classes Slippage, corrosion, crack.

Layout written: data/raw/corrosion/<split>/{images,labels}/<file>. Idempotent: files already
present with the right size are skipped. Standard library only. Also installs requirements.txt
when run as a CAI job (once per requirements hash), like CXR's cxr-setup-data.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from common import finish, parse_args  # noqa: E402

REPO = "LibreYOLO/corrosion-bi3q3"
REVISION = "d20b5b1e6b21ff7de555bbf848dcf7d30ccf8ee7"
LICENCE = "CC-BY-4.0"
API = f"https://huggingface.co/api/datasets/{REPO}/revision/{REVISION}?blobs=true"
FILE = f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/" + "{path}"


def _get(url: str, timeout: int):
    """GET with the read token from OGX_HF_TOKEN (or HF_TOKEN) when set; anonymous otherwise."""
    token = os.environ.get("OGX_HF_TOKEN") or os.environ.get("HF_TOKEN")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    return urllib.request.urlopen(req, timeout=timeout)


def listing() -> list[dict]:
    with _get(API, 60) as r:
        siblings = json.load(r)["siblings"]
    return [s for s in siblings if s["rfilename"].split("/")[0] in ("train", "valid", "test")
            or s["rfilename"] in ("data.yaml", "README.dataset.txt", "README.roboflow.txt")]


def fetch(entry: dict, out: Path) -> bool:
    target = out / entry["rfilename"]
    if target.exists() and (entry.get("size") is None or target.stat().st_size == entry["size"]):
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    url = FILE.format(path=urllib.request.quote(entry["rfilename"]))
    for attempt in range(8):
        try:
            with _get(url, 120) as r:
                data = r.read()
            target.write_bytes(data)
            return True
        except Exception as e:
            if attempt == 7:
                raise
            # HF downloads are rate limited (429), anonymous ones hardest: back off and retry
            time.sleep(min(60, 2 ** attempt) if getattr(e, "code", None) == 429 else 1)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/corrosion")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--no-install", action="store_true")
    args = parse_args(ap)
    out = ROOT / args.out
    files = listing()
    print(f"{REPO}@{REVISION[:7]} ({LICENCE}): {len(files)} files -> {out}", flush=True)
    with ThreadPoolExecutor(args.workers) as pool:
        got = sum(pool.map(lambda e: fetch(e, out), files))
    (out / "SOURCE.json").write_text(json.dumps({"repo": REPO, "revision": REVISION, "licence": LICENCE,
                                                 "files": len(files)}, indent=1))
    print(f"downloaded {got}, already present {len(files) - got}")
    if os.environ.get("CDSW_PROJECT_ID") and not args.no_install:
        from ci.sync_code import install_requirements

        install_requirements(ROOT)
    return 0


if __name__ == "__main__":
    finish(main())
