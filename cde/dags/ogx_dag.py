"""
Airflow DAG (CDE): one batch of the O&G Asset Integrity Lakehouse, one business date per run.

  land_sources -> ingest_bronze -> extract_unstructured -> build_silver -> build_asset_master
    -> build_gold -> reconcile -> batch_complete

land_sources stands in for the source systems (EAM, work orders, inspection PDFs, drawings,
seismic, well logs, drone video) dropping their files on the landing zone. The sensor stream
does not go through this DAG: the long-running jobs rsingh-ogx-stream-{bronze,agg} keep
bronze.sensor_reading and silver.sensor_window current, and build_gold reads the windows
    10|closed before the business date's cut-off. reconcile runs whatever happened upstream (trigger
rule all_done), so a failed batch still gets its report in ref.recon_results; batch_complete
needs every stage to have succeeded, so the run itself is red for a failed batch.

The semantic layer runs on CDW Impala after the batch:
  python scripts/run_semantic.py --engine impala --dates <date>

Not scheduled: trigger it per business date (Trigger DAG w/ config), in date order:
  {"business_date": "2026-10-08"}
Job names must match cde/scripts/deploy_jobs.sh (CDEJobRunOperator fails with 404 otherwise).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

JOB_PREFIX = "rsingh-ogx"
DB_PREFIX = "rsingh_ogx"
LANDING = "s3a://federal-buk-574bcea0/data/IB/rsingh_ogx/landing"
BUSINESS_DATE = "{{ params.business_date }}"

default_args = {
    "owner": "integrity-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    # the virtual cluster is shared: a stage stuck in "starting" must not hold quota for days
    "execution_timeout": timedelta(minutes=90),
}

with DAG(
    dag_id="ogx_asset_integrity",
    description="Structured and unstructured O&G sources -> bronze / silver / asset master / gold on Iceberg",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    params={"business_date": "2026-10-08", "bronze_mode": "normal"},
    tags=["lakehouse", "iceberg", "unstructured", "asset-master", "reconciliation"],
) as dag:

    def stage(task_id: str, job: str, *extra: str, **kwargs) -> CDEJobRunOperator:
        # run-time args replace the job's own, so every task passes the full set
        return CDEJobRunOperator(
            task_id=task_id,
            job_name=f"{JOB_PREFIX}-{job}",
            overrides={"spark": {"args": ["--business-date", BUSINESS_DATE, "--landing", LANDING,
                                          "--db-prefix", DB_PREFIX, "--pipeline-run", "{{ run_id }}", *extra]}},
            wait=True,
            **kwargs,
        )

    land = stage("land_sources", "land")
    bronze = stage("ingest_bronze", "bronze", "--mode", "{{ params.bronze_mode }}")
    extract = stage("extract_unstructured", "extract")
    silver = stage("build_silver", "silver")
    asset = stage("build_asset_master", "asset")
    gold = stage("build_gold", "gold")
    recon = stage("reconcile", "recon", trigger_rule="all_done", retries=0)
    batch_complete = EmptyOperator(task_id="batch_complete", trigger_rule="all_success")

    land >> bronze >> extract >> silver >> asset >> gold >> recon
    [gold, recon] >> batch_complete
