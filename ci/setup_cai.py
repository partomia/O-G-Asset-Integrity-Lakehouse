#!/usr/bin/env python3
"""
One-off CAI setup over the API v2, from a laptop. Idempotent: each step finds by name first
and only creates what is missing. Copied from CXR; the repository is public, so the project is
created straight from its URL (no deploy key, no git archive upload).

  1. Project rsingh-og-asset-integrity from the public GitHub repo, if absent.
  2. Project environment: HF_HOME, the workload user (Impala, Schema Registry), the Kafka
     connection and the environment's CA certificate text (from the caller's environment and
     OGX_KAFKA_CA_PEM; values never printed).
  3. The jobs of ci/cai_jobs.py with parents, schedule, timeout, size and environment.
  4. --app: the application OGX Integrity Workbench.

  set -a; source .env; set +a
  python ci/setup_cai.py --dry-run
  python ci/setup_cai.py
  python ci/setup_cai.py --run ogx-setup-data
  python ci/setup_cai.py --run ogx-stream-producer --env OGX_PRODUCER_MODE=live,OGX_PRODUCER_DURATION=120
  python ci/setup_cai.py --app
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci.cai_jobs import JOBS, RUNTIME, SCORE_JOB  # noqa: E402

CAI_PROJECT_NAME = "rsingh-og-asset-integrity"
GIT_URL = "https://github.com/partomia/O-G-Asset-Integrity-Lakehouse"
PROJECT_ENV = {"HF_HOME": "/home/cdsw/.hf_cache"}
PROJECT_ENV_FROM_CALLER = ("OGX_IMPALA_USER", "OGX_IMPALA_PASSWORD", "OGX_WORKLOAD_USER", "OGX_WORKLOAD_PASSWORD",
                           "OGX_KAFKA_BOOTSTRAP", "OGX_KAFKA_SECURITY_PROTOCOL", "OGX_KAFKA_SASL_MECHANISM",
                           "OGX_KAFKA_USER", "OGX_KAFKA_PASSWORD", "OGX_SCHEMA_REGISTRY_URL", "OGX_HF_TOKEN")
APP = {"name": "OGX Integrity Workbench", "subdomain": "rsingh-ogx-workbench", "script": "app/launch_app.py",
       "cpu": 1, "memory": 4, "description": "Integrity engineer's worklist, Asset 360, document search, feedback"}


class Workbench:
    def __init__(self, url: str, key: str):
        self.base = f"{url.rstrip('/')}/api/v2"
        self.h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def __call__(self, method: str, path: str, params=None, body=None) -> dict:
        r = requests.request(method, self.base + path, params=params, json=body, headers=self.h, timeout=120)
        if r.status_code >= 400:
            raise SystemExit(f"{method} {path}: HTTP {r.status_code} {r.text[:300]}")
        return r.json() if r.text else {}


def find_project(wb: Workbench, name: str = CAI_PROJECT_NAME) -> dict | None:
    found = wb("GET", "/projects", params={"search_filter": json.dumps({"name": name}), "page_size": 100})
    return next((p for p in found.get("projects", []) if p["name"] == name), None)


def ensure_project(wb: Workbench, dry_run: bool) -> dict | None:
    project = find_project(wb)
    if project:
        print(f"project {CAI_PROJECT_NAME}: exists ({project['id']})")
        return project
    if dry_run:
        print(f"project {CAI_PROJECT_NAME}: would create from {GIT_URL}")
        return None
    project = wb("POST", "/projects", body={
        "name": CAI_PROJECT_NAME, "template": "git", "git_url": GIT_URL, "visibility": "private",
        "default_project_engine_type": "ml_runtime",
        "description": "O&G asset integrity on Cloudera AI and the lakehouse (github.com/partomia/O-G-Asset-Integrity-Lakehouse)"})
    print(f"project {CAI_PROJECT_NAME}: created ({project['id']}), cloning", end="", flush=True)
    for _ in range(60):
        status = str(wb("GET", f"/projects/{project['id']}").get("creation_status", "")).lower()
        if status in ("success", "succeeded", ""):
            break
        if "fail" in status or "error" in status:
            raise SystemExit(f"\nproject creation {status}")
        print(".", end="", flush=True)
        time.sleep(5)
    print(" done")
    return project


def wanted_env() -> dict:
    missing = [k for k in PROJECT_ENV_FROM_CALLER if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"set {missing} in the environment (source .env) first")
    env = {**PROJECT_ENV, **{k: os.environ[k] for k in PROJECT_ENV_FROM_CALLER}}
    ca = os.path.expanduser(os.environ.get("OGX_KAFKA_CA_PEM", ""))
    if ca and os.path.exists(ca):
        env["OGX_KAFKA_CA_PEM_TEXT"] = Path(ca).read_text()   # a public CA certificate, not a secret
    return env


def ensure_env(wb: Workbench, project: dict, dry_run: bool) -> None:
    current = json.loads(wb("GET", f"/projects/{project['id']}").get("environment") or "{}")
    wanted = wanted_env()
    changed = sorted(k for k, v in wanted.items() if current.get(k) != v)
    if not changed:
        print("project environment: up to date")
    elif dry_run:
        print(f"project environment: would set {changed}")
    else:
        wb("PATCH", f"/projects/{project['id']}", body={"environment": json.dumps({**current, **wanted})})
        print(f"project environment: set {changed}")


def job_ids(wb: Workbench, pid: str) -> dict:
    return {j["name"]: j for j in wb("GET", f"/projects/{pid}/jobs", params={"page_size": 200}).get("jobs", [])}


def job_env(job: dict) -> dict:
    env = job.get("environment") or {}
    return json.loads(env) if isinstance(env, str) else dict(env)


def ensure_jobs(wb: Workbench, project: dict, dry_run: bool) -> dict:
    pid = project["id"]
    existing = job_ids(wb, pid)
    ids = {n: j["id"] for n, j in existing.items()}
    for job in JOBS:     # parents come first in JOBS
        size = {"cpu": job["cpu"], "memory": job["memory"]}
        env = job.get("env", {})
        if job["name"] in existing:
            have = existing[job["name"]]
            have_env = job_env(have)
            patch = {**({} if {k: have.get(k) for k in size} == size else size),
                     **({} if all(have_env.get(k) == v for k, v in env.items()) else
                        {"environment": json.dumps({**have_env, **env})})}
            if not patch:
                print(f"job {job['name']}: exists ({have['id']})")
            elif dry_run:
                print(f"job {job['name']}: would update {sorted(patch)}")
            else:
                wb("PATCH", f"/projects/{pid}/jobs/{have['id']}", body=patch)
                print(f"job {job['name']}: updated {sorted(patch)}")
            continue
        if not (Path(__file__).resolve().parents[1] / job["script"]).exists():
            print(f"job {job['name']}: skipped ({job['script']} not in this checkout)")
            continue
        if dry_run:
            print(f"job {job['name']}: would create ({job['script']}, parent {job['parent']}, "
                  f"{job['cpu']} vCPU / {job['memory']} GB, env {env})")
            continue
        body = {"name": job["name"], "script": job["script"], **size, "runtime_identifier": RUNTIME,
                "timeout": job["timeout"], "kill_on_timeout": bool(job["timeout"]), "arguments": "",
                "environment": env}
        if job["parent"]:
            body["parent_job_id"] = ids[job["parent"]]
        if job["schedule"]:
            body["schedule"] = job["schedule"]
        created = wb("POST", f"/projects/{pid}/jobs", body=body)
        ids[job["name"]] = created["id"]
        print(f"job {job['name']}: created ({created['id']})")
    return ids


def ensure_app(wb: Workbench, project: dict, dry_run: bool) -> None:
    pid = project["id"]
    app = next((a for a in wb("GET", f"/projects/{pid}/applications", params={"page_size": 100})
                .get("applications", []) if a["name"] == APP["name"]), None)
    if app:
        print(f"application {APP['name']}: exists ({app['id']}, {app.get('status')})")
    elif dry_run:
        print(f"application {APP['name']}: would create ({APP['script']}, {APP['cpu']} vCPU / {APP['memory']} GB)")
    else:
        app = wb("POST", f"/projects/{pid}/applications", body={
            "project_id": pid, "name": APP["name"], "subdomain": APP["subdomain"], "script": APP["script"],
            "cpu": APP["cpu"], "memory": APP["memory"], "kernel": "python3", "runtime_identifier": RUNTIME,
            "description": APP["description"]})
        print(f"application {APP['name']}: created ({app['id']}), subdomain {APP['subdomain']}")


def run_job(wb: Workbench, pid: str, job_id: str, name: str, env: dict | None = None, poll: int = 30) -> str:
    run = wb("POST", f"/projects/{pid}/jobs/{job_id}/runs", body={"environment": env or {}})
    print(f"{name}: run {run['id']} started", flush=True)
    t0, last = time.time(), None
    while True:
        time.sleep(poll)
        st = str(wb("GET", f"/projects/{pid}/jobs/{job_id}/runs/{run['id']}").get("status", "")).lower()
        st = st.replace("engine_", "")
        if st != last:
            print(f"{name}: {st} after {time.time() - t0:.0f} s", flush=True)
            last = st
        if st in ("succeeded", "failed", "stopped", "timedout"):
            return st


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--app", action="store_true")
    p.add_argument("--run", default="", help="start this job by name and follow it to the end")
    p.add_argument("--env", default="", help="with --run: KEY=VALUE,... for the job run's environment")
    p.add_argument("--poll", type=int, default=30)
    args, _ = p.parse_known_args()
    wb = Workbench(os.environ["OGX_CAI_HOST"], os.environ["OGX_CAI_API_KEY"])
    project = ensure_project(wb, args.dry_run)
    if project is None:
        return 0
    if args.run:
        job = job_ids(wb, project["id"]).get(args.run)
        if not job:
            raise SystemExit(f"no job {args.run}")
        env = dict(kv.split("=", 1) for kv in args.env.split(",") if kv)
        return 0 if run_job(wb, project["id"], job["id"], args.run, env, args.poll) == "succeeded" else 1
    ensure_env(wb, project, args.dry_run)
    ids = ensure_jobs(wb, project, args.dry_run)
    if args.app:
        ensure_app(wb, project, args.dry_run)
    print(f"\nGitHub secrets: CAI_URL = {os.environ['OGX_CAI_HOST'].rstrip('/')}, CAI_PROJECT_ID = {project['id']}")
    print(f"Airflow Variables: OGX_CAI_PROJECT_ID = {project['id']}, "
          f"OGX_CAI_SCORE_JOB_ID = {ids.get(SCORE_JOB, '(not created)')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
