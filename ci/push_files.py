#!/usr/bin/env python3
"""
Upload repository files to the CAI project through the API v2 files endpoint, for when
ogx-00-sync-code (git reset to origin/main) cannot be scheduled. Only tracked files are sent;
data, models and outputs never leave the laptop this way.

  set -a; source .env; set +a
  python ci/push_files.py --since HEAD~1          # files changed in the last commit
  python ci/push_files.py app/app.py config/guardrails.yaml
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ci.setup_cai import Workbench, find_project  # noqa: E402


def changed(since: str) -> list[str]:
    out = subprocess.check_output(["git", "diff", "--name-only", "--diff-filter=AM", since, "HEAD"], cwd=ROOT, text=True)
    return [p for p in out.split() if (ROOT / p).is_file()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*")
    ap.add_argument("--since", default="")
    a = ap.parse_args()
    paths = sorted(set(a.paths + (changed(a.since) if a.since else [])))
    tracked = set(subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True).split())
    skipped = [p for p in paths if p not in tracked]
    paths = [p for p in paths if p in tracked and (ROOT / p).stat().st_size > 0]
    if skipped:
        print(f"not tracked, not sent: {skipped}")
    wb = Workbench(os.environ["OGX_CAI_HOST"], os.environ["OGX_CAI_API_KEY"])
    pid = find_project(wb)["id"]
    url = f"{wb.base}/projects/{pid}/files"
    h = {"Authorization": wb.h["Authorization"]}
    for p in paths:
        with open(ROOT / p, "rb") as f:
            r = requests.put(url, headers=h, files={p: (Path(p).name, f)}, timeout=120)
        print(f"{'ok ' if r.ok else 'ERR'} {p}" + ("" if r.ok else f" HTTP {r.status_code} {r.text[:150]}"), flush=True)
        if not r.ok:
            return 1
    print(f"{len(paths)} files uploaded to project {pid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
