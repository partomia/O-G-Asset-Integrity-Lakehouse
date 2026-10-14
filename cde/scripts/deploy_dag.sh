#!/usr/bin/env bash
# Register/update the Airflow DAG as a `--type airflow` CDE job sourced from the repository
# (copied from GDL). Re-run after every DAG change: `cde repository sync` alone does not refresh a
# registered DAG. No schedule: trigger it per business date (see cde/dags/ogx_dag.py).
#
#   ./cde/scripts/deploy_dag.sh
#   cde job run --name rsingh-ogx-orchestration --config-json '{"business_date": "2026-10-08"}'

set -euo pipefail

REPO_NAME="${REPO_NAME:-rsingh-ogx-pipeline}"
DAG_JOB_NAME="${DAG_JOB_NAME:-rsingh-ogx-orchestration}"
DAG_PATH="cde/dags/ogx_dag.py"

cde repository sync --name "${REPO_NAME}"

if cde job describe --name "${DAG_JOB_NAME}" &>/dev/null; then
  echo "==> Updating ${DAG_JOB_NAME}"
  cde job update --name "${DAG_JOB_NAME}" --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
  echo "Give Airflow ~30s to re-parse before triggering."
  exit 0
fi

echo "==> Creating ${DAG_JOB_NAME}"
cde job create --name "${DAG_JOB_NAME}" --type airflow --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
echo "DAG ogx_asset_integrity registered (no schedule). Trigger one business date:"
echo "  cde job run --name ${DAG_JOB_NAME} --config-json '{\"business_date\": \"2026-10-08\"}'"
