#!/usr/bin/env bash
# Create/sync the CDE Repository for this public GitHub repo and create (or update in place) one
# Spark job per pipeline stage, each reading its application file from the repo (mounted at
# /app/mount, so the jobs find config/, contracts/, generators/ and extract/ next to cde/jobs/).
# The jobs need only PySpark and the standard library: no python-env resource. Copied from GDL;
# the repo is public, so no deploy key and no files-resource upload of the code.
#
# After a code change: git push, then  cde repository sync --name rsingh-ogx-pipeline
#
# Resources: larger than GDL's (approved): a 2-core / 4 GB driver and 1 to 4 executors of
# 4 cores / 8 GB. Nothing is deleted: an existing job is updated.
#
#   set -a; source .env; set +a; ./cde/scripts/deploy_jobs.sh

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/O-G-Asset-Integrity-Lakehouse}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-ogx-pipeline}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-ogx}"
DB_PREFIX="${DB_PREFIX:-rsingh_ogx}"
LANDING="${LANDING:-s3a://federal-buk-574bcea0/data/IB/rsingh_ogx/landing}"
RESOURCES=(--driver-cores "${DRIVER_CORES:-2}" --driver-memory "${DRIVER_MEMORY:-4g}"
           --executor-cores "${EXECUTOR_CORES:-4}" --executor-memory "${EXECUTOR_MEMORY:-8g}"
           --min-executors 1 --initial-executors 1 --max-executors "${MAX_EXECUTORS:-4}"
           --conf spark.sql.shuffle.partitions=16
           --conf spark.sql.adaptive.enabled=true)

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  cde repository create --name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}"
fi
cde repository sync --name "${REPO_NAME}"

create_job() {
  local name=$1 file=$2
  if [[ ! -f "${file}" ]]; then echo "==> Skipping ${name}: ${file} not in this checkout"; return; fi
  if cde job describe --name "${name}" &>/dev/null; then
    echo "==> Updating job ${name} (${file})"
    cde job update --name "${name}" --application-file "${file}" "${RESOURCES[@]}" >/dev/null
  else
    echo "==> Creating job ${name} (${file})"
    cde job create --name "${name}" --type spark \
      --mount-1-resource "${REPO_NAME}" \
      --application-file "${file}" \
      "${RESOURCES[@]}" \
      --arg=--db-prefix --arg="${DB_PREFIX}" --arg=--landing --arg="${LANDING}"
  fi
}

create_job "${JOB_PREFIX}-land"     "cde/jobs/land_sources.py"
create_job "${JOB_PREFIX}-bronze"   "cde/jobs/ingest_bronze.py"
create_job "${JOB_PREFIX}-extract"  "cde/jobs/extract_unstructured.py"
create_job "${JOB_PREFIX}-silver"   "cde/jobs/build_silver.py"
create_job "${JOB_PREFIX}-asset"    "cde/jobs/build_asset_master.py"
create_job "${JOB_PREFIX}-gold"     "cde/jobs/build_gold.py"
create_job "${JOB_PREFIX}-outcomes" "cde/jobs/build_outcomes.py"
create_job "${JOB_PREFIX}-recon"    "cde/jobs/reconcile.py"

echo ""
echo "Jobs deployed from ${REPO_NAME}. One stage by hand (run-time args replace the job's):"
echo "  cde job run --name ${JOB_PREFIX}-bronze --arg=--business-date --arg=2026-10-04 \\"
echo "    --arg=--db-prefix --arg=${DB_PREFIX} --arg=--landing --arg=${LANDING} --wait"
echo "Streaming jobs: ./cde/scripts/deploy_streaming.sh create|start|stop|restart|status"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"
