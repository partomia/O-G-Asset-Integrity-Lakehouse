"""
CAI Application entry point (copied from CXR): Applications > New Application > Script =
app/launch_app.py, Python 3.11 runtime, 2 vCPU / 8 GB. Also works from a session terminal.
"""
import os
import subprocess
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    # A CAI Application with a JupyterLab kernel runs this inside IPython,
    # where __file__ is undefined; CAI sets the cwd to the project root.
    try:
        return Path(__file__).resolve().parent.parent
    except NameError:
        cwd = Path.cwd()
        return cwd if (cwd / "app" / "app.py").exists() else Path("/home/cdsw")


ROOT = _repo_root()
PORT = os.environ.get("CDSW_APP_PORT", "8090")
HOST = os.environ.get("OGX_APP_HOST", "127.0.0.1")

cmd = [sys.executable, "-m", "streamlit", "run", str(ROOT / "app" / "app.py"),
       "--server.port", PORT, "--server.address", HOST, "--server.headless", "true",
       "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
       "--browser.gatherUsageStats", "false", "--server.fileWatcherType", "none",
       "--server.runOnSave", "false"]
# Run Streamlit as a child and block: exec-ing would replace the Jupyter kernel process, which
# CAI treats as the application dying. The kernel outlives a crashed child, so CAI keeps showing
# the application as running while nothing listens on the port: restart the child instead.
env = {**os.environ, "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS", "1"),
       "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS", "1"),
       "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS", "1")}
restarts = 0
while True:
    print(f"[launch_app] {' '.join(cmd)}  (cwd={ROOT}, restart {restarts})", flush=True)
    t0 = time.time()
    code = subprocess.call(cmd, cwd=ROOT, env=env)
    print(f"[launch_app] streamlit exited with status {code} after {time.time() - t0:.0f} s", flush=True)
    restarts = 0 if time.time() - t0 > 300 else restarts + 1
    if restarts >= 5:
        raise SystemExit("streamlit keeps exiting at start: giving up")
    time.sleep(3)
