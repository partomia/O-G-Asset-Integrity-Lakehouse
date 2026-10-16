"""
CAI job stop-ogx-project: a one-off, scheduled end of the demo. Runs
`scripts/platform.py stop --only cai,nifi` (application, model deployment, ogx-* schedules and
runs, the NiFi process group), then pauses its own schedule so it fires once.

CDE needs nothing here: the DAG's end_date ends the nightly runs and no streaming job runs
between them. The name does not start with ogx-, so the CAI layer does not stop this run.
Restart everything later from a laptop: python scripts/platform.py start.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

JOB_NAME = "stop-ogx-project"


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path.cwd()


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import importlib.util  # noqa: E402

# loaded by path: scripts/ on sys.path would shadow the standard library's platform module
spec = importlib.util.spec_from_file_location("ogx_platform", ROOT / "scripts" / "platform.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)


def main() -> int:
    rc = P.main(["stop", "--only", "cai,nifi"] + (["--dry-run"] if os.environ.get("OGX_STOP_DRY_RUN") == "1" else []))
    if os.environ.get("OGX_STOP_DRY_RUN") != "1":
        c = P.Cai()
        for j in c("GET", c.p("/jobs"), params={"page_size": 200})["jobs"]:
            if j["name"] == JOB_NAME and not j.get("paused"):
                c("PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": True})
                print(f"paused the schedule of {JOB_NAME}", flush=True)
    return rc


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
