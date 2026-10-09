"""
Job ogx-04-deploy-champion (runs only if ogx-03-kpi-gate succeeded). Copied from CXR's
deploy_champion.py and trimmed:
  1. archive the current champion, promote the gated candidate to serving.champion_dir
  2. pin requirements-model.txt to this environment's scikit-learn (the one that trained the
     model), then build and deploy serve/predict.py as CAI Model ogx-integrity with the
     Cloudera AI API v2 (cmlapi); the build snapshots the project files
  3. if the build or deployment fails, put the previous champion back
Inside a CAI job, cmlapi.default_client() authenticates as the job: no API key in the project.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from common import finish, load_config  # noqa: E402


def promote(cfg) -> tuple[dict, Path | None]:
    cand = ROOT / cfg["serving"]["candidate_dir"]
    gate_path = cand / "gate_result.json"
    gate = json.loads(gate_path.read_text()) if gate_path.exists() else {"passed": False}
    meta = json.loads((cand / "model_meta.json").read_text())
    if not gate["passed"] or gate.get("candidate_git_sha") != meta["git_sha"]:
        raise SystemExit("Candidate has no passing gate result - refusing to deploy.")
    target = ROOT / cfg["serving"]["champion_dir"]
    archived = None
    if target.exists():
        archived = ROOT / cfg["serving"]["archive_dir"] / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-champion")
        archived.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(target), str(archived))
        print(f"archived previous champion -> {archived}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(cand, target)
    return meta, archived


def rollback(cfg, archived: Path | None) -> None:
    target = ROOT / cfg["serving"]["champion_dir"]
    shutil.rmtree(target, ignore_errors=True)
    if archived and archived.exists():
        shutil.move(str(archived), str(target))
        print(f"rolled back: previous champion restored from {archived}")


def pin_requirements() -> None:
    import sklearn

    (ROOT / "requirements-model.txt").write_text(f"numpy\npillow\nscikit-learn=={sklearn.__version__}\njoblib\npyyaml\n")
    print(f"requirements-model.txt: scikit-learn=={sklearn.__version__}", flush=True)


def wait(fn, ok: set, what: str, timeout=2400):
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        status = str(fn().status or "").lower()
        if status != last:
            print(f"  {what}: {status}", flush=True)
            last = status
        if status in ok:
            return status
        if "fail" in status or status in {"stopped", "timedout"}:
            raise RuntimeError(f"{what} ended in status {status}")
        time.sleep(20)
    raise RuntimeError(f"{what} timed out after {timeout} s")


def deploy(cfg, meta) -> None:
    import cmlapi

    from ci.cai_jobs import resolve_runtime

    client = cmlapi.default_client()
    pid = os.environ["CDSW_PROJECT_ID"]
    name = cfg["serving"]["model_name"]
    runtime = resolve_runtime(client, cfg["cai"]["runtime_identifier"])
    found = [m for m in client.list_models(pid, search_filter=json.dumps({"name": name})).models if m.name == name]
    model = found[0] if found else client.create_model(cmlapi.CreateModelRequest(
        project_id=pid, name=name, description="O&G asset integrity: corrosion severity on drone keyframes",
        disable_authentication=False), pid)
    print(f"model {model.name} id={model.id}, runtime {runtime}", flush=True)
    t = meta["metrics"]["test"]
    build = client.create_model_build(cmlapi.CreateModelBuildRequest(
        project_id=pid, model_id=model.id, file_path="serve/predict.py", function_name="predict",
        runtime_identifier=runtime,
        comment=f"{meta['estimator']} {meta['feature_version']} git {meta['git_sha'][:7]} test auroc {t['auroc']:.3f}",
    ), pid, model.id)
    wait(lambda: client.get_model_build(pid, model.id, build.id), {"built", "succeeded"}, "build")
    dep = client.create_model_deployment(cmlapi.CreateModelDeploymentRequest(
        project_id=pid, model_id=model.id, build_id=build.id,
        cpu=cfg["cai"]["model_cpu"], memory=cfg["cai"]["model_memory_gb"]), pid, model.id, build.id)
    wait(lambda: client.get_model_deployment(pid, model.id, build.id, dep.id), {"deployed"}, "deployment")
    print(f"champion deployed: model {model.id} build {build.id} deployment {dep.id} "
          f"(access key {model.access_key})", flush=True)


def main() -> int:
    cfg = load_config()
    meta, archived = promote(cfg)
    print(f"champion: {meta['model']} {meta['estimator']}, test AUROC {meta['metrics']['test']['auroc']:.3f}", flush=True)
    pin_requirements()
    try:
        deploy(cfg, meta)
    except Exception as e:  # noqa: BLE001
        print(f"deploy failed: {e}", flush=True)
        rollback(cfg, archived)
        return 1
    return 0


if __name__ == "__main__":
    finish(main())
