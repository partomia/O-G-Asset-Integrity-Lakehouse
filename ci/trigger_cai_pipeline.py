"""
Runs on the GitHub Actions runner (copied from CXR, one chain here). Talks ONLY to the
Cloudera AI API v2 (REST):
  1. finds the corrosion chain's CAI jobs by name (ci/cai_jobs.py, CHAIN)
  2. starts ogx-00-sync-code with the commit SHA (EXPECTED_GIT_SHA) and OGX_TRIGGER; CAI job
     dependencies start ogx-01 build-features -> 02 train-validate -> 03 KPI gate -> 04 deploy
  3. follows each run until it succeeds or fails, and names the KPI gate when that is what
     stopped the chain (the serving champion keeps its place). The check goes red on any failure.
Images and model training never leave the Cloudera AI Workbench.

Env: CAI_URL, CAI_API_KEY, CAI_PROJECT_ID, GITHUB_SHA (set by Actions), optional CAI_CA_BUNDLE.
Without CAI_URL it prints a notice and succeeds, so the workflow stays green until the secrets
are added.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci.cai_jobs import CHAIN, GATE_JOB, JOBS  # noqa: E402

OK = {"succeeded"}
BAD = {"failed", "stopped", "timedout", "killed"}
TIMEOUTS = {j["name"]: j["timeout"] + 1800 for j in JOBS}   # job timeout + queueing margin
POLL_S = 15


def status_of(run: dict) -> str:
    return str(run.get("status", "")).lower().replace("engine_", "")


def parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


class Api:
    def __init__(self, url: str, key: str, project: str, verify):
        self.base = f"{url.rstrip('/')}/api/v2/projects/{project}"
        self.h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        self.verify = verify

    def __call__(self, method: str, path: str, **kw) -> dict:
        r = requests.request(method, f"{self.base}{path}", headers=self.h, verify=self.verify, timeout=60, **kw)
        r.raise_for_status()
        return r.json() if r.text else {}

    def job_ids(self, wanted: list[str]) -> dict:
        jobs = self("GET", "/jobs", params={"page_size": 200}).get("jobs", [])
        ids = {j["name"]: j["id"] for j in jobs}
        missing = [n for n in wanted if n not in ids]
        if missing:
            sys.exit(f"::error::CAI jobs not found: {missing}. Run ci/setup_cai.py first.")
        return {n: ids[n] for n in wanted}

    def latest_run(self, job_id: str) -> dict | None:
        runs = self("GET", f"/jobs/{job_id}/runs", params={"sort": "-created_at", "page_size": 1}).get("job_runs", [])
        return runs[0] if runs else None


def follow(api: Api, name: str, job_id: str, since: datetime, run_id: str | None = None) -> str:
    """Wait for this job's run (the given run id, else the first run created after `since`)."""
    t0, last = time.time(), None
    while time.time() - t0 < TIMEOUTS[name]:
        run = api("GET", f"/jobs/{job_id}/runs/{run_id}") if run_id else api.latest_run(job_id)
        created = parse_ts(run.get("created_at")) if run else None
        if run and (run_id or (created and created >= since)):
            st = status_of(run)
            if st != last:
                print(f"  {name}: {st} (run {run.get('id')})", flush=True)
                last = st
            if st in OK or st in BAD:
                return st
        time.sleep(POLL_S)
    print(f"  {name}: no result after {TIMEOUTS[name]} s")
    return "timedout"


def summary(rows: list[tuple[str, str]]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write("| CAI job | Result |\n|---|---|\n" + "".join(f"| `{n}` | {s} |\n" for n, s in rows))


def main() -> int:
    url = os.environ.get("CAI_URL", "")
    if not url:
        print("::notice::CAI_URL secret not set - skipping the CAI pipeline (unit tests still ran).")
        return 0
    api = Api(url, os.environ["CAI_API_KEY"], os.environ["CAI_PROJECT_ID"], os.environ.get("CAI_CA_BUNDLE") or True)
    try:
        ids = api.job_ids(CHAIN)
    except (requests.ConnectionError, requests.Timeout) as e:
        print(f"::error::Cannot reach {url} from this runner ({type(e).__name__}). A workbench on a private "
              "network needs a self-hosted runner inside it: set the repository variable CAI_RUNS_ON "
              "to [\"self-hosted\",\"cai\"].")
        return 1
    sha = os.environ.get("GITHUB_SHA", "")
    env = {"EXPECTED_GIT_SHA": sha[:12], "OGX_TRIGGER": f"github push {sha[:7]}"}
    run = api("POST", f"/jobs/{ids[CHAIN[0]]}/runs", json={"environment": env})
    since = (parse_ts(run.get("created_at")) or datetime.now(timezone.utc)) - timedelta(seconds=5)
    print(f"commit {sha[:7]}: started {CHAIN[0]} run {run.get('id')}", flush=True)
    rows = []
    for i, name in enumerate(CHAIN):
        st = follow(api, name, ids[name], since, run.get("id") if i == 0 else None)
        rows.append((name, st))
        if st not in OK:
            stage = "KPI GATE REJECTED the candidate" if name == GATE_JOB else f"{name} {st}"
            print(f"::error::{stage}. The serving champion keeps its place. See the job log in Cloudera AI.")
            rows += [(n, "not run") for n in CHAIN[i + 1:]]
            summary(rows)
            return 1
    summary(rows)
    print("pipeline succeeded: the new champion is deployed and registered (ref.model_event)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
