# Reuse inventory

Everything this repo takes from the two sibling demos that already run on the federal
environment: General-DataLakehouse (GDL) and Chest-X-ray-Triage (CXR). No secret values
appear here; secrets live only in the git-ignored `.env` (copied from the siblings' `.env`
files, renamed to `OGX_*`) and in CAI project / Airflow variables.

## Connections, credentials, conventions, helpers

| What | Where found (repo:path) | New OGX_ env var or config key | Reused as-is or adapted |
|---|---|---|---|
| CDW Impala host `coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site:443`, HTTP transport, `http_path=cliservice`, LDAP | GDL:config/pipeline.json `impala`; CXR:config/lakehouse.json `impala` | `config/lakehouse.json` `impala` | as-is |
| Impala workload user and password | GDL:.env `GDL_IMPALA_*`; CXR:.env `CXR_IMPALA_*` (identical values) | `OGX_IMPALA_USER`, `OGX_IMPALA_PASSWORD` | as-is (renamed) |
| Impala client: impyla 0.21.0, `use_ssl`, `use_http_transport`; ImpalaStore / SparkStore over one interface; `REFRESH`, `DESCRIBE HISTORY`, `FOR SYSTEM_VERSION AS OF` | CXR:lakehouse/store.py; GDL:scripts/run_semantic.py `ImpalaEngine` | `lakehouse/store.py`, `scripts/run_semantic.py` | adapted (OGX tables, `OGX_*` env) |
| Impala cannot write Iceberg `timestamptz`: ref tables Impala writes use `timestamp_ntz` holding UTC | GDL:cde/jobs/gdl_common.py, docs/PROJECT_LOG.md | `cde/jobs/ogx_common.py` schemas | as-is |
| Impala reserved words (`change`, `matched`) must not be aliases | GDL:docs/PROJECT_LOG.md, tests SQL portability | `tests/test_semantic_sql.py` | as-is |
| CAI workbench host (`https://...federal.dp5i-5vkq.cloudera.site`) | GDL:.env `GDL_CAI_HOST`; CXR:.env `CXR_CAI_HOST` (identical) | `OGX_CAI_HOST` | as-is (renamed) |
| CAI API v2 key | GDL:.env `GDL_CAI_API_KEY`; CXR:.env `CXR_CAI_API_KEY` (identical) | `OGX_CAI_API_KEY`; GitHub secret `CAI_API_KEY` | as-is (renamed) |
| CAI runtime identifier `docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5`, `resolve_runtime()` paging `list_runtimes` | CXR:ci/cai_jobs.py `RUNTIME`, `resolve_runtime` | `ci/cai_jobs.py` | as-is |
| CAI project setup over API v2 (find by name, create from Git, env vars, jobs, app, `--run`) | CXR:ci/setup_cai.py `Workbench` | `ci/setup_cai.py`, project `rsingh-og-asset-integrity` | adapted: created from the **public** URL; deploy-key bootstrap dropped |
| CAI job chain: parents, sizes 2 vCPU / 8 GB (4 vCPU never schedules), timeouts in seconds, env picks the model | CXR:ci/cai_jobs.py, docs/PROJECT_LOG.md | `ci/cai_jobs.py` jobs `ogx-00`..`ogx-07`, `ogx-setup-data`, `ogx-stream-producer`; env `OGX_MODEL` | adapted |
| GitHub -> CAI API v2 trigger, status normalised `lower().replace("engine_","")`, notice until `CAI_URL` set, `CAI_RUNS_ON` | CXR:ci/trigger_cai_pipeline.py, .github/workflows/cai-mlops.yml | `ci/trigger_cai_pipeline.py`, `.github/workflows/cai-mlops.yml`; secrets `CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID` | adapted |
| Sync-code job: `git reset --hard origin/main`, `EXPECTED_GIT_SHA`, pip install once per requirements hash | CXR:ci/sync_code.py | `ci/sync_code.py` (`ogx-00-sync-code`) | as-is |
| Feature hash, versioned feature table + manifest, data checks, stub embedder for CI | CXR:features/feature_logic.py, features/build_feature_table.py, config/ci.yaml | `features/*`, `config/ci.yaml` | adapted (keyframes + sensor windows) |
| ViT backbone `google/vit-base-patch16-224` pinned at `3f49326eb077187dfe1c2a2bb15fbd74e6ab91e3`, CPU wheels | CXR:config/pipeline.yaml, requirements.txt | `config/pipeline.yaml` `features` | as-is |
| KPI gate (absolute + non-regression + hash + no `--limit`), exit 1 stops the chain | CXR:gate/kpi_gate.py | `gate/kpi_gate.py` | adapted (per-head KPIs) |
| AI Registry: `create_registered_model` per stage, tags, never fails a job; MLflow `set_experiment` before reopening a run | CXR:serve/registry.py | `serve/registry.py`; models `ogx-corrosion`, `ogx-frame-qc`, `ogx-equipment-risk` | as-is (+ Iceberg snapshot id tag) |
| Deploy champion with rollback; 500 from `create_model_deployment` after 30 s gateway deadline = look for the deployment | CXR:serve/deploy_champion.py | `serve/deploy_champion.py` | as-is |
| Silent trial -> champion with evidence and a named approver | CXR:serve/promote_champion.py | `serve/promote_champion.py`, env `OGX_APPROVED_BY` | adapted |
| Lineage publish only inside a CAI job (`CDSW_PROJECT_ID`); conftest strips platform credentials | CXR:lakehouse/publish.py, docs/PROJECT_LOG.md | `lakehouse/publish.py`, `tests/conftest.py` | as-is |
| Streamlit app as a child process of `launch_app.py` (CORS/XSRF off); stop the app while jobs run (quota) | CXR:app/launch_app.py, docs/PROJECT_LOG.md | `app/launch_app.py` | as-is |
| CDE virtual cluster endpoint `https://bxjjm2cr.cde-lzjl69mv.federal.dp5i-5vkq.cloudera.site/dex/api/v1` (service `federal-cde`) | ~/.cde/config.yaml | `OGX_CDE_VCLUSTER_ENDPOINT`; CLI profile unchanged (`~/.cde/config.yaml`, user rsingh) | as-is |
| CDE: Spark 3.5.4, Iceberg 1.5.2, `spark_catalog` Hive-backed; tables only through the common helper (`using("iceberg")`) | GDL:docs/PROJECT_LOG.md, cde/jobs/gdl_common.py | `cde/jobs/ogx_common.py` | as-is |
| CDE Git repository resource + one Spark job per stage, 1 core / 2 GB driver, 1-2 executors | GDL:cde/scripts/deploy_jobs.sh | `cde/scripts/deploy_jobs.sh`, resource `rsingh-ogx-pipeline`, jobs `rsingh-ogx-*` | as-is (GDL pattern; CXR's `git archive` files resource dropped, repo is public) |
| Airflow DAG as `--type airflow` job, paused, no schedule; `batch_complete` all_success; recon all_done | GDL:cde/dags/gdl_dag.py, cde/scripts/deploy_dag.sh | `cde/dags/ogx_dag.py`, job `rsingh-ogx-orchestration` | adapted (+ CAI scoring task) |
| DAG -> CAI job run with env, poll status | CXR:cde/dags/cxr_dag.py `trigger_cai_score` | `cde/dags/ogx_dag.py` task `cai_score_inspections` | adapted |
| Airflow Variables over the Airflow REST API with a Knox token for the workload user; only own keys touched | CXR:cde/scripts/set_airflow_variables.py | `cde/scripts/set_airflow_variables.py`; variables `OGX_CAI_HOST`, `OGX_CAI_PROJECT_ID`, `OGX_CAI_API_KEY`, `OGX_CAI_SCORE_JOB_ID` | adapted |
| `cde job run --wait` hangs on a dropped network: submit, then poll | GDL:docs/PROJECT_LOG.md | `cde/scripts/*` | as-is |
| Data Visualization URL `https://viz-indianbank-spend-analytics.dw-federal-cdp-env.dp5i-5vkq.cloudera.site` (8.1.4), connection `federal-impala-1` | GDL:config/pipeline.json `dataviz_url`; CXR:config/lakehouse.json `dataviz_connection` | `config/lakehouse.json` | as-is |
| Data Visualization API key (SAML: passwords give 401) | GDL:.env `GDL_VIZ_API_KEY`; CXR:.env `CXR_VIZ_API_KEY` (identical) | `OGX_VIZ_API_KEY` | as-is (renamed) |
| Dashboards as code: export JSON, `/arc/migration/api/import/`, verify every visual through `/arc/api/data` | GDL:dataviz/build_dashboard.py | `dataviz/build_dashboard.py` | adapted; **fixed PKs 13000+** (see the plan correction below) |
| Knox gateway of the data lake `https://federal-aw-dl-gateway.federal.dp5i-5vkq.cloudera.site/federal-aw-dl/cdp-proxy-api` (Atlas `/atlas/api/atlas/v2`, Ranger `/ranger/service/public/v2/api`), basic auth | GDL:config/pipeline.json `datalake_api`, scripts/governance.py | `config/lakehouse.json` `datalake_api`; `OGX_WORKLOAD_USER`, `OGX_WORKLOAD_PASSWORD` | as-is |
| Atlas: Iceberg tables are `iceberg_table`/`iceberg_column`, views `hive_table`/`hive_column`; cluster `cm` | GDL:scripts/governance.py, docs/PROJECT_LOG.md | `scripts/governance.py` | as-is |
| Ranger tag service `cm_tag` (linked to `cm_hive`); masked demo users `federal01`, `federal07`; `rsingh` sees clear | GDL:config/governance.json `masking` | `config/governance.json`: `OGX_SENSITIVE_*`, policies `rsingh-ogx-*`, glossary `OGX Integrity KPIs` | adapted |
| Data lake CRN | GDL:.env `GDL_DL_CRN` | `OGX_DL_CRN` | as-is (renamed) |
| S3 bucket and path convention `s3a://federal-buk-574bcea0/data/IB/rsingh_<code>/{landing,reports}/<source>/<date>/` + `_manifest.json` | GDL:cde/jobs/gdl_common.py; CXR:config/lakehouse.json | `s3a://federal-buk-574bcea0/data/IB/rsingh_ogx/{landing,raw,checkpoints,reports}/` | adapted (+ `raw/`, `checkpoints/`) |
| Databases `<prefix>_<layer>`, prefix on the database only; `Names`, `Audit`, `load_audit`, `transform_log`, `recon_results`, MATCHED / EXPLAINED / MISMATCH | GDL:cde/jobs/gdl_common.py, cde/jobs/reconcile.py | `rsingh_ogx_{bronze,silver,asset,gold,semantic,ref}` | adapted |
| Bronze: contracts, quarantine with reason codes, `overwritePartitions` per date, `--mode fail-during/resume` drill | GDL:cde/jobs/ingest_bronze.py | `cde/jobs/ingest_bronze.py` | adapted (object catalog) |
| Iceberg catalog for local Spark: `local` catalog, JDBC SQLite (views) or Hadoop; Spark 4.0.1 + `iceberg-spark-runtime-4.0_2.13:1.10.0`; `OGX_ICEBERG_PACKAGE` + `OGX_LOCAL_CATALOG=hadoop` reproduce CDE's 3.5 / 1.5.2 | GDL:scripts/run_local.py `local_spark` | `scripts/run_local.py` | as-is (renamed env) |
| Semantic runner: one SQL dialect on Impala and Spark, `-- name:` blocks, `${...}` params, prefix rewrite | GDL:scripts/run_semantic.py | `scripts/run_semantic.py` | adapted |
| Jobs import with no SparkSession (no Column at module load) | GDL:tests/test_job_imports.py | `tests/test_job_imports.py` | as-is |
| Iceberg reserves `_file`: lineage column is `_source_file` | CXR:docs/PROJECT_LOG.md | bronze schemas | as-is |
| `commit-msg` hook (strips co-author / tool attribution trailers) | GDL:.git/hooks/commit-msg | `scripts/hooks/commit-msg` (+ gitleaks `pre-commit`) | as-is |
| `.gitignore` for data / features / archive / outputs, champion kept | CXR:.gitignore, tests/test_cai_contract.py | `.gitignore` (+ `checkpoints/`, `*.jks`, `*.keytab`, `*.pem`) | adapted |
| Kafka brokers `federal-kafka-corebroker{0,1,2}.federal.dp5i-5vkq.cloudera.site:9093`, SASL_SSL, PAM | found read-only: `cdp datahub describe-cluster --cluster-name federal-kafka` (not in the siblings) | `OGX_KAFKA_BOOTSTRAP`, `OGX_KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `OGX_KAFKA_SASL_MECHANISM=PLAIN` | new |
| Kafka workload user / password (same CDP workload user) | GDL/CXR .env workload pair | `OGX_KAFKA_USER`, `OGX_KAFKA_PASSWORD` | as-is (assumed: CDP Data Hub Kafka accepts the workload password over SASL PLAIN) |
| Truststore: environment root CA (FreeIPA, valid to 2046) | `cdp environments get-root-certificate --environment-name federal-cdp-env` (public cert) | `OGX_KAFKA_CA_PEM` (local file `~/.ogx/federal-cdp-env-ca.pem`, never in Git) | new |
| Schema Registry `https://federal-kafka-master0.federal.dp5i-5vkq.cloudera.site/federal-kafka/cdp-proxy-api/schema-registry/api/v1`; SMM API `.../cdp-proxy-api/smm-api` (Knox, PAM) | `cdp datahub describe-cluster` | `OGX_SCHEMA_REGISTRY_URL`, `OGX_SMM_API_URL` | new |

| Kafka Connect `https://federal-kafka-master0.federal.dp5i-5vkq.cloudera.site/federal-kafka/cdp-proxy-api/kafka-connect/` | given by Ravi 2026-10-08 (matches `describe-cluster`) | `config/streaming.json` `kafka_connect_url` | new, not used by the plan (noted as an option) |
| NiFi Data Hub `federal-nifi`: REST `https://federal-nifi-management0.federal.dp5i-5vkq.cloudera.site/federal-nifi/cdp-proxy-api/nifi-app/nifi-api/`, Registry `.../nifi-registry-app/nifi-registry-api/`, Schema Registry `.../federal-nifi/cdp-proxy-api/schema-registry/api/v1/`, nodes `federal-nifi-nifi{0,1,2}` | given by Ravi 2026-10-08 | `config/streaming.json` `nifi` | new, not used by the plan: an alternative producer path (NiFi -> Kafka) for the docs |

### CAI runtime lessons enforced by `test_cai_contract.py` (carried over as-is)

| Lesson | Where it is handled here |
|---|---|
| Job kernel runs scripts without `__file__` | `_repo_root()` with `except NameError` in every job script |
| ... and passes an extra `-f <file>` | `common.parse_args()` uses `parse_known_args` |
| Any `SystemExit`, even `sys.exit(0)`, is a failed run | `common.finish()`; `if rc: sys.exit(rc)` only |
| Run status `ENGINE_SUCCEEDED`; job-run `arguments` ignored, `environment` applied | trigger and DAG normalise status; business date / model passed in the environment |
| `exec`-ing Streamlit kills the app kernel | `app/launch_app.py` child process |
| mlflow pinned in requirements breaks `mlflow-cml-plugin` | no mlflow in `requirements.txt` (contract test) |
| Workflow red before secrets exist | `cai-mlops.yml` notice until `CAI_URL` is set |
| `parent_job_id` and `schedule` are exclusive; parents created first | `ci/cai_jobs.py` + contract test |
| `list_runtimes` is paged | `resolve_runtime` pages |
| 500 after the 30 s gateway deadline on `create_model_deployment` | `deploy_champion.find_deployment` |

## Found while reading (plan corrections)

1. **Data Visualization PKs 9500+ collide with GDL.** GDL's export uses dashboards 9000+, datasets
   9100+, visuals 9200 + 100 per dashboard (max PK 9508 in `gdl_dashboards.json`). CXR uses 12000+.
   Proposed: OGX at **13000+** (dashboards 13000, datasets 13100, visuals 13200+).
2. **Kafka brokers are not reachable from the laptop** (port 9093 closed from outside), so the
   producer runs as CAI job `ogx-stream-producer` (as planned) and local streaming tests use a
   local broker (Homebrew Kafka, KRaft).
3. **CAI quota**: one 2 vCPU workload beside the model endpoint (CXR log). The producer job, the
   scoring job and the application cannot run at the same time; the runbook sequences them.
4. **GPU**: none usable (4 vCPU requests never left scheduling on this workbench), so the corrosion
   head is frozen embeddings + a small head, as the plan proposes.
5. **Streams Messaging Data Hub exists**: `federal-kafka` (Light Duty, CDP 7.3.2, AVAILABLE, with
   Schema Registry, SMM, Cruise Control). There is also `federal-flink` (SSB) for the docs' alternative.

## Still missing (only you can give)

1. Kafka authorisation for `rsingh` on `federal-kafka`: can it create topics `ogx.*` and
   produce/consume them (Ranger `cm_kafka` policies), and use Schema Registry? If not, who adds
   a policy (or creates the three topics)?
2. Confirm the workload password is the Kafka SASL PLAIN credential (no keytab needed).
3. Network path CAI -> brokers :9093 and CDE -> brokers :9093 (both inside the environment; I
   will test it as the first live step if you approve).
4. CDE: may a Spark job run continuously for the demo window (no virtual-cluster job timeout)?
   I will read it from `cde job describe` / a 10-minute run unless you know.
