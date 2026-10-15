#!/usr/bin/env python3
"""
Stop, start or show the whole demo across every layer, in dependency order (config/platform.json).

Two scopes:
  project   (default) this demo's workloads only: CAI application, model deployments, scheduled
            and running ogx-* jobs; CDE streaming and running rsingh-ogx-* runs; the NiFi process
            group. Kafka topics, Iceberg tables, checkpoints and Schema Registry stay as they are,
            so a start resumes from the checkpoints with no gap and no duplicate.
  platform  the project scope plus the environment's shared services: Data Hubs (Kafka, NiFi,
            Flink), the CDW Impala virtual warehouse, the CDE virtual cluster and the CAI
            workspace. Sibling demos use the same services, so this needs --yes-shared.

Stop runs in stop_order (consumers before producers: CAI, NiFi, CDE, CDW, Flink, Kafka); start
in start_order (Kafka first, CAI last), each layer's service before its workloads. --only limits
the layers. Data Visualization has no suspend in the CDP CLI and is left running.

  set -a; source .env; set +a
  python scripts/platform.py status [--scope platform]
  python scripts/platform.py stop  [--only cai,cde] [--dry-run]
  python scripts/platform.py start
  python scripts/platform.py stop --scope platform --yes-shared
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
PRJ, PLT = CFG["project"], CFG["platform"]
LAYERS = ("kafka", "flink", "cdw", "cde", "nifi", "cai")
DRY = False


def say(msg: str) -> None:
    print(msg, flush=True)


def act(what: str, fn, *a, **kw):
    if DRY:
        say(f"  would {what}")
        return None
    say(f"  {what}")
    return fn(*a, **kw)


def wait_for(what: str, fn, ok: set, timeout: int = 2400, poll: int = 20) -> str:
    if DRY:
        return ""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        st = str(fn())
        if st != last:
            say(f"    {what}: {st} ({time.time() - t0:.0f} s)")
            last = st
        if st.lower() in {s.lower() for s in ok}:
            return st
        time.sleep(poll)
    raise SystemExit(f"{what}: still {last} after {timeout} s")


# ------------------------------------------------------------------ CLIs
def cdp(*args: str) -> dict:
    exe = os.environ.get("OGX_CDP_CLI") or shutil.which("cdp") or next(iter(glob.glob(
        os.path.expanduser("~/Library/Python/*/bin/cdp"))), None)
    if not exe:
        raise SystemExit("cdp CLI not found: pip install cdpcli, or set OGX_CDP_CLI")
    out = subprocess.run([exe, *args], capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError(f"cdp {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def cde(*args: str):
    out = subprocess.run(["cde", *args], capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError(f"cde {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout) if out.stdout.strip().startswith(("{", "[")) else out.stdout


class Cai:
    def __init__(self):
        self.base = os.environ["OGX_CAI_HOST"].rstrip("/") + "/api/v2"
        self.h = {"Authorization": f"Bearer {os.environ['OGX_CAI_API_KEY']}"}
        found = self("GET", "/projects", params={"search_filter": json.dumps({"name": PRJ["cai_project"]})})
        self.pid = next(p["id"] for p in found["projects"] if p["name"] == PRJ["cai_project"])

    def __call__(self, method: str, path: str, **kw) -> dict:
        r = requests.request(method, self.base + path, headers=self.h, timeout=60, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json() if r.text.strip() else {}

    def p(self, path: str) -> str:
        return f"/projects/{self.pid}{path}"

    def apps(self) -> list[dict]:
        return [a for a in self("GET", self.p("/applications"), params={"page_size": 100})["applications"]
                if a["name"] in PRJ["cai_apps"]]

    def jobs(self) -> list[dict]:
        return [j for j in self("GET", self.p("/jobs"), params={"page_size": 200})["jobs"]
                if j["name"].startswith(PRJ["cai_job_prefix"])]

    def active_runs(self, job: dict) -> list[dict]:
        runs = self("GET", self.p(f"/jobs/{job['id']}/runs"), params={"page_size": 20}).get("job_runs", [])
        return [r for r in runs if r["status"] in ("ENGINE_SCHEDULING", "ENGINE_STARTING", "ENGINE_RUNNING")]

    def deployments(self) -> list[tuple[dict, dict, dict]]:
        """(model, build, deployment) for every deployment of the project's models, newest first."""
        out = []
        for m in self("GET", self.p("/models"), params={"page_size": 100})["models"]:
            if m["name"] not in PRJ["cai_models"]:
                continue
            for b in self("GET", self.p(f"/models/{m['id']}/builds"), params={"page_size": 100})["model_builds"]:
                for d in self("GET", self.p(f"/models/{m['id']}/builds/{b['id']}/deployments"))["model_deployments"]:
                    out.append((m, b, d))
        return sorted(out, key=lambda t: t[2]["created_at"], reverse=True)


# ------------------------------------------------------------------ project workloads
def cai_project(action: str) -> None:
    c = Cai()
    deps = c.deployments()
    if action == "status":
        for a in c.apps():
            say(f"  app {a['name']}: {a['status']} ({a['cpu']} vCPU / {a['memory']} GB)")
        for name in PRJ["cai_models"]:
            live = [(b, d) for m, b, d in deps if m["name"] == name and d["status"] == "deployed"]
            say(f"  model {name}: " + (f"deployed (build {live[0][0]['id'][:8]})" if live else "not deployed"))
        for j in c.jobs():
            runs = c.active_runs(j)
            if j.get("schedule") or runs:
                say(f"  job {j['name']}: schedule {j.get('schedule') or '-'}{' PAUSED' if j.get('paused') else ''}"
                    f"{', ' + str(len(runs)) + ' active run(s)' if runs else ''}")
        return
    if action == "stop":
        for a in c.apps():
            if a["status"] != "APPLICATION_STOPPED":
                act(f"stop application {a['name']}", c, "POST", c.p(f"/applications/{a['id']}:stop"))
        for m, b, d in deps:
            if d["status"] in ("deployed", "deploying", "pending"):
                act(f"stop model {m['name']} deployment {d['id'][:8]}", c, "POST",
                    c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:stop"))
        for j in c.jobs():
            if j["name"] in PRJ["cai_scheduled_jobs"] and not j.get("paused"):
                act(f"pause schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": True})
            for r in c.active_runs(j):
                act(f"stop job run {j['name']} {r['id']}", c, "POST", c.p(f"/jobs/{j['id']}/runs/{r['id']}:stop"))
        return
    # start: schedules, the newest deployment of each model, then the applications
    for j in c.jobs():
        if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("paused"):
            act(f"resume schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": False})
    for name in PRJ["cai_models"]:
        mine = [(m, b, d) for m, b, d in deps if m["name"] == name]
        if mine and mine[0][2]["status"] != "deployed":
            m, b, d = mine[0]
            act(f"restart model {name} deployment {d['id'][:8]}", c, "POST",
                c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:restart"))
    for a in c.apps():
        if a["status"] != "APPLICATION_RUNNING":
            act(f"start application {a['name']}", c, "POST", c.p(f"/applications/{a['id']}:restart"))


def cde_project(action: str) -> None:
    runs = [r for r in cde("run", "list", "--filter", "status[in]running,starting", "--filter",
                           f"job[rlike]^{PRJ['cde_job_prefix']}") or []]
    scheduled = [j for j in cde("job", "list", "--filter", f"name[rlike]^{PRJ['cde_job_prefix']}") or []
                 if (j.get("schedule") or {}).get("enabled")]
    if action == "status":
        say(f"  active runs: " + (", ".join(f"{r['job']} #{r['id']} {r['status']}" for r in runs) or "none"))
        for j in scheduled:
            say(f"  schedule {j['name']}: {'paused' if j['schedule'].get('paused') else 'active'}")
        missing = [j for j in PRJ["cde_streaming_jobs"] if j not in {r["job"] for r in runs}]
        if missing:
            say(f"  streaming not running: {', '.join(missing)}")
        return
    if action == "stop":
        for j in scheduled:
            if not j["schedule"].get("paused"):
                act(f"pause CDE schedule {j['name']}", cde, "job", "schedule", "pause", "--name", j["name"])
        # Spark children first, so an orchestration run does not retry them
        for r in sorted(runs, key=lambda r: r["type"] == "airflow"):
            act(f"kill CDE run {r['job']} #{r['id']}", cde, "run", "kill", "--id", str(r["id"]))
        return
    for j in scheduled:
        if j["schedule"].get("paused"):
            act(f"resume CDE schedule {j['name']}", cde, "job", "schedule", "resume", "--name", j["name"])
    running = {r["job"] for r in runs}
    if all(j in running for j in PRJ["cde_streaming_jobs"]):
        say("  streaming jobs already running")
        return
    act("start the streaming jobs (cde/scripts/deploy_streaming.sh start, resumes from the checkpoints)",
        subprocess.run, [str(ROOT / "cde" / "scripts" / "deploy_streaming.sh"), "start"], check=True)
    say("  the producer is not started: cde job run --name rsingh-ogx-stream-produce "
        "--arg=--mode --arg=live --arg=--duration --arg=10800")


def nifi_project(action: str) -> None:
    from nifi.deploy_flow import Nifi

    n = Nifi()
    for name in PRJ["nifi_process_groups"]:
        g = n.find_group(name)
        if g is None:
            say(f"  process group {name}: not found")
            continue
        if action == "status":
            say(f"  process group {name}: {g.get('runningCount', 0)} running, {g.get('stoppedCount', 0)} stopped, "
                f"{g.get('invalidCount', 0)} invalid")
            continue
        state = "STOPPED" if action == "stop" else "RUNNING"
        act(f"{action} NiFi process group {name}", n.call, "PUT", f"/flow/process-groups/{g['id']}",
            json={"id": g["id"], "state": state})
        if action == "start" and not DRY:
            # "Hold failures" processors stay stopped by design, so failures stay queued and visible
            procs = n.call("GET", f"/process-groups/{g['id']}/processors")["processors"]
            for p in procs:
                if p["component"]["name"].startswith("Hold"):
                    n.call("PUT", f"/processors/{p['id']}/run-status",
                           json={"revision": p["revision"], "state": "STOPPED"})


PROJECT = {"cai": cai_project, "cde": cde_project, "nifi": nifi_project}


# ------------------------------------------------------------------ shared services
def datahub(layer: str, action: str) -> None:
    name = PLT["datahubs"].get(layer)
    if not name:
        return
    status = lambda: cdp("datahub", "describe-cluster", "--cluster-name", name)["cluster"]["status"]  # noqa: E731
    st = status()
    if action == "status":
        say(f"  Data Hub {name}: {st}")
    elif action == "stop" and st != "STOPPED":
        act(f"stop Data Hub {name}", cdp, "datahub", "stop-cluster", "--cluster-name", name)
        wait_for(name, status, {"STOPPED"})
    elif action == "start" and st != "AVAILABLE":
        act(f"start Data Hub {name}", cdp, "datahub", "start-cluster", "--cluster-name", name)
        wait_for(name, status, {"AVAILABLE"}, timeout=3600)


def cdw(action: str) -> None:
    env_crn = cdp("environments", "describe-environment", "--environment-name", CFG["environment"])["environment"]["crn"]
    cid = next(c["id"] for c in cdp("dw", "list-clusters")["clusters"] if c["environmentCrn"] == env_crn)
    vws = {v["name"]: v for v in cdp("dw", "list-vws", "--cluster-id", cid)["vws"]}
    for name in PLT["cdw_vws"]:
        vw = vws[name]
        status = lambda v=vw: cdp("dw", "describe-vw", "--cluster-id", cid, "--vw-id", v["id"])["vw"]["status"]  # noqa: E731
        if action == "status":
            say(f"  CDW virtual warehouse {name}: {vw['status']}")
        elif action == "stop" and vw["status"].lower() not in ("stopped", "suspended"):
            act(f"suspend CDW virtual warehouse {name}", cdp, "dw", "suspend-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Stopped", "Suspended"})
        elif action == "start" and vw["status"].lower() != "running":
            act(f"start CDW virtual warehouse {name}", cdp, "dw", "start-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Running"})
    if action == "status":
        say("  Data Visualization: no suspend in the CDP CLI, left as is")


def cde_vc(action: str) -> None:
    svc = next(s for s in cdp("de", "list-services")["services"]
               if s["name"] == PLT["cde_service"] and s["status"] == "ClusterCreationCompleted")
    vc = next(v for v in cdp("de", "list-vcs", "--cluster-id", svc["clusterId"])["vcs"] if v["vcName"] == PLT["cde_vc"])
    ids = ("--cluster-id", svc["clusterId"], "--vc-id", vc["vcId"])
    status = lambda: cdp("de", "describe-vc", *ids)["vc"]["status"]  # noqa: E731
    st = vc["status"]
    if action == "status":
        say(f"  CDE virtual cluster {PLT['cde_vc']}: {st}")
    elif action == "stop" and "suspend" not in st.lower():
        act(f"suspend CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "suspend-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppSuspended", "Suspended"})
    elif action == "start" and "suspend" in st.lower():
        act(f"resume CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "resume-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppInstalled", "Running"})


def cai_workspace(action: str) -> None:
    def ws():
        return next(w for w in cdp("ml", "list-workspaces")["workspaces"]
                    if w["instanceName"] == PLT["cai_workspace"] and w["environmentName"] == CFG["environment"])
    w = ws()
    st = w["instanceStatus"]
    if action == "status":
        say(f"  CAI workspace {PLT['cai_workspace']}: {st}")
    elif action == "stop" and "suspend" not in st:
        act(f"suspend CAI workspace {PLT['cai_workspace']}", cdp, "ml", "suspend-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"suspend:finished"}, timeout=3600)
    elif action == "start" and "suspend" in st:
        act(f"resume CAI workspace {PLT['cai_workspace']}", cdp, "ml", "resume-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"resume:finished", "installation:finished"},
                 timeout=3600)


SHARED = {"kafka": lambda a: datahub("kafka", a), "flink": lambda a: datahub("flink", a),
          "nifi": lambda a: datahub("nifi", a), "cdw": cdw, "cde": cde_vc, "cai": cai_workspace}


def layer(name: str, action: str, scope: str) -> None:
    say(f"[{name}]")
    steps = []
    if action == "start" and scope == "platform":
        steps.append(SHARED[name])
    if name in PROJECT:
        steps.append(PROJECT[name])
    if action != "start" and scope == "platform":
        steps.append(SHARED[name])
    for step in steps:
        try:
            step(action)
        except Exception as e:  # noqa: BLE001  one layer's failure is reported; the others still run
            say(f"  ERROR {type(e).__name__}: {e}")
            if action != "status":
                raise


def main() -> int:
    global DRY
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "stop", "start"])
    ap.add_argument("--scope", choices=["project", "platform"], default="project")
    ap.add_argument("--only", default="", help=f"comma-separated layers from {', '.join(LAYERS)}")
    ap.add_argument("--yes-shared", action="store_true", help="required to stop or start the shared services")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    DRY = a.dry_run
    if a.scope == "platform" and a.action != "status" and not (a.yes_shared or a.dry_run):
        raise SystemExit("--scope platform stops/starts services other demos share: add --yes-shared")
    only = {x.strip() for x in a.only.split(",") if x.strip()}
    if only - set(LAYERS):
        raise SystemExit(f"unknown layers {sorted(only - set(LAYERS))}")
    order = CFG["start_order"] if a.action == "start" else CFG["stop_order"]
    for name in order:
        if not only or name in only:
            layer(name, a.action, a.scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())
