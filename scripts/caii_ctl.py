#!/usr/bin/env python3
"""
Cloudera AI Inference for this project (config/inference.json): the service, its compute
cluster, the LLM in the AI Registry and the endpoint the Agentic Integrity Monitor calls.

  set -a; source .env; set +a
  python scripts/caii_ctl.py status               # cluster, service, registry version, endpoint
  python scripts/caii_ctl.py import               # registry_repo from Hugging Face into the AI Registry
  python scripts/caii_ctl.py deploy               # endpoint from the newest READY registry version
  python scripts/caii_ctl.py scale --min 0        # 0: no GPU node (the next call cold-starts); 1: warm
  python scripts/caii_ctl.py chat "Say OK"        # one completion through agents/llm.py
  python scripts/caii_ctl.py delete               # the endpoint only; the service and model stay

The service itself was created once with `cdp compute create-cluster` and `cdp ml
create-ml-serving-app` (docs/AGENTIC_MONITOR.md has the payloads). Calls use the CDP JWT.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1] if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(ROOT))
CFG_PATH = ROOT / "config" / "inference.json"
CFG = json.loads(CFG_PATH.read_text())
EP = CFG["endpoint"]


def cdp(*args: str) -> dict:
    exe = os.environ.get("OGX_CDP_CLI") or shutil.which("cdp") or next(
        iter(sorted(Path.home().glob("Library/Python/*/bin/cdp"))), None)
    if not exe:
        raise SystemExit("cdp CLI not found: pip install cdpcli, or set OGX_CDP_CLI")
    out = subprocess.run([str(exe), *args], capture_output=True, text=True, timeout=300)
    if out.returncode:
        raise RuntimeError(f"cdp {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout or "{}")


def token() -> str:
    from agents.llm import cdp_token

    if not os.environ.get("OGX_CAII_TOKEN") and not Path("/tmp/jwt").exists():
        os.environ["OGX_CAII_TOKEN"] = cdp("iam", "generate-workload-auth-token", "--workload-name", "DE")["token"]
    return cdp_token()


def service() -> dict | None:
    return next((a for a in cdp("ml", "list-ml-serving-apps").get("apps", [])
                 if a["appName"] == CFG["app_name"]), None)


def domain() -> str:
    """config/inference.json `domain`, else the service's: a CAI job has a JWT but maybe no cdp CLI."""
    return CFG.get("domain") or service()["cluster"]["domainName"]


def registry() -> dict | None:
    return next((r for r in cdp("ml", "list-model-registries").get("modelRegistries", [])
                 if r.get("environmentName") == CFG["environment"]), None)


class Serving:
    def __init__(self, domain: str):
        self.base = f"https://{domain}"
        self.h = {"Authorization": f"Bearer {token()}", "Content-Type": "application/json"}

    def __call__(self, op: str, body: dict) -> dict:
        r = requests.post(f"{self.base}/api/v1alpha1/{op}", json=body, headers=self.h, timeout=120)
        if r.status_code >= 400:
            raise SystemExit(f"{op}: HTTP {r.status_code} {r.text[:400]}")
        return r.json() if r.text.strip() else {}

    def endpoint(self) -> dict | None:
        eps = self("listEndpoints", {"namespace": CFG["namespace"]}).get("endpoints", [])
        return next((e for e in eps if e.get("name") == EP["name"]), None)


def registry_version(reg: dict) -> tuple[dict, dict] | None:
    r = requests.get(f"{reg['domain']}/api/v2/models", headers={"Authorization": f"Bearer {token()}"}, timeout=60)
    r.raise_for_status()
    model = next((m for m in r.json().get("models", []) if m["name"] == CFG["registry_model"]), None)
    if not model:
        return None
    full = requests.get(f"{reg['domain']}/api/v2/models/{model['id']}", headers={"Authorization": f"Bearer {token()}"},
                        timeout=60).json()
    versions = sorted(full.get("model_versions") or [], key=lambda v: v["version"])
    return (full, versions[-1]) if versions else (full, {})


def import_model() -> None:
    reg = registry()
    if registry_version(reg):
        print(f"registry model {CFG['registry_model']}: exists")
        return
    r = requests.post(f"{reg['domain']}/api/v2/models", headers={"Authorization": f"Bearer {token()}"}, timeout=120, json={
        "name": CFG["registry_model"],
        "createModelVersionRequestPayload": {
            "metadata": {"model_repo_type": "HF"},
            "downloadModelRepoRequest": {"source": "HF", "repo_id": CFG["registry_repo"]}}})
    if r.status_code >= 400:
        raise SystemExit(f"import: HTTP {r.status_code} {r.text[:400]}")
    print(f"registry model {CFG['registry_model']}: importing {CFG['registry_repo']}; poll with `status`")


def status() -> dict:
    out: dict = {}
    cl = next((c for c in cdp("compute", "list-clusters", "--env-name-or-crn", CFG["environment"]).get("clusters", [])
               if c.get("clusterName") == CFG["compute_cluster"]), None)
    out["compute_cluster"] = f"{cl['clusterName']} {cl['status']}" if cl else "absent"
    svc = service()
    out["service"] = f"{svc['appName']} {svc['status']} ({svc['cluster']['domainName']})" if svc else "absent"
    reg = registry()
    rv = registry_version(reg) if reg else None
    out["registry_model"] = (f"{rv[0]['name']} v{rv[1].get('version')} {rv[1].get('status')}" if rv and rv[1]
                             else "absent")
    if svc and svc["status"].endswith("finished") and svc["cluster"].get("domainName"):
        e = Serving(svc["cluster"]["domainName"]).endpoint()
        out["endpoint"] = (f"{e['name']} {e.get('status', {}).get('state') if isinstance(e.get('status'), dict) else e.get('state')}"
                           f" replicas {e.get('replica_count', e.get('replicas'))} url {e.get('url')}") if e else "absent"
    for k, v in out.items():
        print(f"{k:16s} {v}")
    return out


def deploy() -> None:
    svc, reg = service(), registry()
    if not svc or not svc["status"].endswith("finished"):
        raise SystemExit(f"service {CFG['app_name']} not ready: {svc and svc['status']}")
    model, ver = registry_version(reg) or (None, None)
    if not ver or ver.get("status") not in ("READY", "REGISTERED"):
        raise SystemExit(f"registry model {CFG['registry_model']} not ready: {ver and ver.get('status')}")
    s = Serving(svc["cluster"]["domainName"])
    if s.endpoint():
        print(f"endpoint {EP['name']}: exists")
        return
    spec = {"namespace": CFG["namespace"], "name": EP["name"],
            "source": {"registry_source": {"model_id": model["id"], "version": ver["version"]}},
            "resources": {"req_cpu": str(EP["req_cpu"]), "req_memory": EP["req_memory"], "num_gpus": str(EP["num_gpus"])},
            "instance_type": EP["instance_type"],
            "autoscaling": {"min_replicas": str(EP["min_replicas"]), "max_replicas": str(EP["max_replicas"])}}
    print(json.dumps(spec, indent=1))
    s("deployEndpoint", spec)
    print(f"endpoint {EP['name']}: deploying; poll with `status`")


def wait_ready(timeout_s: int = 3600) -> dict:
    s = Serving(domain())
    t0, last = time.time(), None
    while time.time() - t0 < timeout_s:
        e = s.endpoint() or {}
        st = e.get("status", {}).get("state") if isinstance(e.get("status"), dict) else e.get("state")
        if st != last:
            print(f"{time.strftime('%H:%M')} endpoint {st}", flush=True)
            last = st
        if str(st).lower() in ("loaded", "running", "ready"):
            return e
        if "fail" in str(st).lower():
            raise SystemExit(json.dumps(e, indent=1)[:2000])
        time.sleep(30)
    raise SystemExit("endpoint not ready in time")


def scale(min_replicas: int) -> None:
    """updateEndpoint replaces the spec, so the deployed source and resources go back with it."""
    s = Serving(domain())
    cur = s("describeEndpoint", {"namespace": CFG["namespace"], "name": EP["name"]})
    s("updateEndpoint", {
        "namespace": CFG["namespace"], "name": EP["name"],
        "source": cur["revisions"][0]["source"],
        "resources": {k: cur["resources"][k] for k in ("req_cpu", "req_memory", "num_gpus")},
        "instance_type": EP["instance_type"],
        "autoscaling": {"min_replicas": str(min_replicas), "max_replicas": str(max(EP["max_replicas"], min_replicas))}})
    print(f"endpoint {EP['name']}: min_replicas {min_replicas}")


def delete() -> None:
    Serving(domain())("deleteEndpoint", {"namespace": CFG["namespace"], "name": EP["name"]})
    print(f"endpoint {EP['name']}: deleted")


def chat(prompt: str) -> None:
    from agents.llm import CAIIChat, Completion

    c, out, t0 = CAIIChat(token=token()), Completion(), time.time()
    for d in c.chat_stream([{"role": "user", "content": prompt}], out, use_cache=False):
        print(d, end="", flush=True)
    print(f"\n[{c.model} {time.time() - t0:.1f} s, {out.usage.total_tokens} tokens]")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cmd", choices=["status", "import", "deploy", "wait", "scale", "delete", "chat"])
    p.add_argument("prompt", nargs="?", default="Reply with the single word OK.")
    p.add_argument("--min", type=int, default=1)
    a = p.parse_args()
    {"status": status, "import": import_model, "deploy": deploy, "wait": wait_ready, "delete": delete,
     "scale": lambda: scale(a.min), "chat": lambda: chat(a.prompt)}[a.cmd]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
