# Cloudera platform reference: symptom -> cause -> fix

Lessons from the reference implementations listed in SKILL.md. Each bullet reads
symptom -> cause -> fix, with the projects where it was seen in brackets. Search this file for the
error text before debugging from scratch; add a bullet when a new failure is solved.

## CAI (workbench, jobs, models, registry, applications)
- Job run reports failed although the script succeeded -> the Jupyter-kernel job wrapper treats any `SystemExit`, even `sys.exit(0)`, as an error -> call `sys.exit(rc)` only when `rc` is non-zero; on success let the script end [Mule, CXR, Spend, OGX].
- Job script crashes at import -> the job kernel runs scripts without `__file__` and passes an extra `-f <file>` argument -> use a `_repo_root()` fallback with `except NameError`, and `parse_known_args()` [Collections, CXR, Spend, OGX, Mule].
- Run parameters never reach the job -> a job run ignores `arguments` but applies `environment` -> pass the run date, trigger source and git SHA as environment variables [CASA, CXR, Spend].
- Status checks miss the end of a run -> statuses come back as `ENGINE_SUCCEEDED`, `ENGINE_TIMEDOUT` and so on -> normalise with `lower().replace("engine_","")` and treat `timedout` as terminal [CXR, OGX].
- Jobs sit in `ENGINE_SCHEDULING` -> requests above what the workbench nodes or the per-user quota can place (8 vCPU / 32 GB for 14-20 min, 4 vCPU for 25-35 min, 2 vCPU / 8 GB for 37 min) -> step down to 4/16, then 2/8, then 1 vCPU / 4 GB until it starts at once [Mule, Spend, Churn, CXR, OGX].
- GPU job stuck in scheduling for 14 min on resource group 0 -> no free GPU on federal -> run on CPU (TabICL at 4 vCPU / 16 GB) [Churn, Collections].
- A job waits until the application is stopped -> the per-user quota fits the model endpoint plus one 2 vCPU workload -> stop the app while chains run, and run chains one after another [CXR, OGX].
- Run ends `timedout` after about 15 min -> the UI default job timeout is 15 (`kill_on_timeout` true) -> set the timeout explicitly, in seconds, through API v2 [Churn, CXR].
- pip killed with no traceback at 1 vCPU / 2 GB while installing torch (553 MB cuDNN wheel) -> out of memory -> give install/sync jobs 2 vCPU / 8 GB; install once per `requirements.txt` hash [Spend].
- Multi-date backfill reports `ENGINE_SUCCEEDED` but stops writing after the first date -> a 16 GB engine runs out of memory and a killed kernel is reported as success -> run one child process per date and fail the job on a non-zero or killed child [Collections, CASA].
- MLflow logging breaks (`unexpected keyword argument 'run_uuid'`, `typing-extensions` pin warning) -> a pip-installed mlflow shadows the runtime's copy and breaks `mlflow-cml-plugin` -> keep mlflow out of requirements; uninstall the user copy [CXR, Churn, OGX].
- `mlflow.get_run` / reopening a run fails with "Missing the required parameter experiment_id" -> the plugin needs the experiment set first -> call `set_experiment`, or look the experiment up by name [CXR].
- Job create/update rejected (`cannot unmarshal ... string`) -> API v2 wants `environment` as an object on create but as a JSON string on update -> send each form where it is expected [CXR, OGX].
- Job create returns 400 "script not found in project directory" -> a job can only be created for a file already in the project -> sync the code first; or upload the file with PUT and a form field named after its path (POST `/files` puts it in the root) [Churn, CXR].
- Reading a project file with GET returns 404 -> files are read with `POST /files/<path>:download`; API v2 has no job-run log endpoint -> write diagnostics to project files, or use the UI for logs [Spend, Mule, Collections].
- The wrong runtime was picked (2025.09 instead of 2026.08) -> `list_runtimes` is paged -> read every page and warn when there is no exact match [CXR, OGX].
- `parent_job_id` combined with `schedule` is rejected -> the two are mutually exclusive -> create parent jobs first, and schedule only the root [OGX].
- A manual run of the sync job also re-ran the whole chain -> child jobs fire after their parent -> expect it, or trigger the leaf jobs directly [CXR].
- `create_model_deployment` returns 500 `context deadline exceeded` after 30 s, yet the deployment proceeds -> the API gateway's 30 s deadline -> on a 5xx or timeout, look up the build's deployments (`list_model_deployments`, up to 5 min) and wait; fail at once only on 4xx [CXR, OGX].
- Model build stuck in `pushing` / `building` (30-45 min) -> a platform stall -> time out and roll back to the champion; wait on the deployment as well as the build; a re-run builds in 4-6 min [CXR, Spend].
- Model build stuck in `pushing` for 11+ hours -> the build image copies every project file not git-ignored, including the workbench home state (`.local` site-packages with torch/scipy/pyarrow, `.cache/pip`), so the image is several GB -> git-ignore `.local/ .cache/ .config/ .ipython/ .jupyter/ .experiments/`; the next build finished normally [OGX].
- Endpoint answers "model_meta.json not found" after a rebuild -> the model folder was git-ignored, and git-ignored files never reach the build -> leave the serving model folder out of `.gitignore` and keep it out of Git with `.git/info/exclude` [OGX].
- PBJ runtime endpoint answers "context deadline exceeded --- model busy" to every call -> the PBJ model server only calls a function wrapped by `cml.models_v1.cml_model` -> decorate `predict` (with a no-op fallback for local imports) [OGX, CXR].
- Model deployed but missing from Registered Models -> registration ran after the deploy step, and a deploy timeout rolled back first -> register the version before deploying [OGX].
- Model deployment stuck in `deploying` with empty logs -> the workbench CPU instance group was at its max -> raise it with `cdp ml modify-cluster-instance-group`; the 500 "not in a modifiable state" still applied the change [Collections].
- A long CAI run ended silently while the workbench was being resized -> the instance-group modify can kill running engines -> do not resize while jobs run [Collections].
- First jobs right after project creation fail with `SSL: UNEXPECTED_EOF_WHILE_READING` / `RemoteDisconnected` -> transient API drops -> make setup idempotent and re-run; retry GETs (10 tries, never on 4xx); never retry POSTs [Collections, Churn].
- Creating a project from a private GitHub URL gives `creation_status: failure` -> CAI cannot clone private repos without credentials -> create a blank project, upload a read-only deploy key, and have a bootstrap job check out `origin/main`; or make the repo public [CXR, OGX].
- Workbench endpoint calls take 2-16 s (TabICL/Mitra) -> the model context is rebuilt or cold -> expect 2-16 s per call; 16.8 s cold, then about 2 s [Spend, Churn, Mule].
- The endpoint accepts the CAI API v2 key as its bearer token -> this works, but the scope is broad -> use a separate Model API key in production [Spend, Churn, Collections, Mule].
- Every endpoint call answers "model busy" / 30 s timeout -> once seen with a private AI Registry, once with a deployment that never warmed -> restart it; if 400s follow, `predict` is raising, so return `{"error": ...}` instead of an opaque 400 [OGX, Spend].
- Registry model answers 400 to every input -> a float32 tensor signature rejects float64 input -> log the model with a float64 signature [Spend].
- Registry model answers 400 to all-zero or whole-number rows -> MLflow schema/type inference on integer-valued JSON -> send health-check rows containing fractional values (0.5) [Spend].
- `GET /api/v2/registry/models` returns 500 and registry models time out -> no AI Registry existed, then a private-access one was used -> create the AI Registry with Public internet access [Spend].
- Tags passed at registry version create or PATCH are not stored, and a PATCH hung for 18 min -> registry limitation -> keep stage and approver in an Iceberg `model_event` table; champion = highest version [Spend, CXR].
- A new version is not found 40 s after it was registered -> registry listing lag -> wait up to 5 min, then fall back to the run date [Spend].
- Workbench model created from the registry cannot be deleted (API 501) and is still listed after a UI delete -> registry model link is sticky -> register under a new name [Spend].
- Build from a registry version fails with 400 "Kernel must be in list [python3 python2 r]" -> the API reports the previous build's kernel as "Python 3.11" -> send `kernel: python3` [Spend].
- Do not set `kernel` when a runtime is given -> API v2 rule -> send the runtime identifier only [CXR].
- The deployment request has no replicas field -> API v2 shape -> send `cpu`, `memory`, `nvidia_gpus` and `environment` only [CXR].
- Streamlit app kernel dies -> `exec`/`os.system` of Streamlit kills the app kernel -> launch it as a child process, with CORS/XSRF off for uploads behind the CAI proxy [CXR, OGX].
- App shows its fallback page -> it started before the project env vars were set -> restart the application after setting the variables [OGX].
- An app or job URL answers 302 -> CAI SSO login -> treat 302 as healthy in smoke tests [CASA, Collections, Mule].
- The Overview showed "Total GPU: 0" for a GPU deployment -> stale overview tile -> check the Deployments tab [Mule].
- A run's `environment` printed to the terminal exposed a token -> job/run objects return env values in clear -> never print run or job environments; rotate the token if it was shown [OGX].
- Local pytest published fake lineage events to the live `model_event` table -> the test shell had sourced `.env` -> publish only inside CAI (`CDSW_PROJECT_ID` set); strip platform credentials in `conftest.py` [CXR].

## CDE (Spark, streaming, Airflow, resources)
- Run rejected: "queue root.dex-app-… cannot fit application" (36 vCPU / 84 GB request) -> the federal vcluster quota is smaller than go01's sizing -> driver 2c/4 GB, executors 4c/8 GB, min 1 / initial 2 / max 4-16 [Churn, Spend, Mule, CASA, Collections].
- generate-bronze failed within 28 s with no logs ("object not found") -> executors of 8c/12 GB x 4 initial were oversized -> same fix: 4c/8 GB, 2 initial [Mule].
- Driver pod waits 16-17 min, then "Initial job has not accepted any resources" for over an hour, or stages run on 1-3 executors -> the vcluster was adding nodes or out of capacity -> poll and wait, or kill and move clusters; a smaller executor size places more easily [Churn, Spend].
- Runs queue 15-25 min behind other demos -> shared vcluster -> combine stages into one Spark session (a `stages` job), and wait for other projects' Spark runs before submitting [OGX, Mule].
- A new DAG's stages sit in "starting" for hours while `cde run list` shows other projects' orchestration and generate-bronze runs "running" for 2-3 days -> stuck runs hold the virtual cluster quota (cpuRequests 20, memRequests 80Gi; 27 CPU / 103 Gi in use), and each nightly schedule adds more -> with the owner's consent kill the stale runs (Spark children first, then the orchestration) and pause idle demos' schedules (`cde job schedule pause`); stages start within a minute. Prevent it with `dagrun_timeout` and `execution_timeout` in every DAG [OGX].
- `cde job run --wait` hangs on a dropped network, or returns while the run is still "starting" -> the wait is client-side -> submit without `--wait`, then poll `cde run describe` [GDL, Spend, CASA, Collections].
- `cde run logs` truncates or garbles driver output (tqdm `\r` lines; a driver crash showed no traceback) -> CDE log service limits -> verify results in Impala/Iceberg tables; reproduce on local PySpark 3.5.4 [Collections, Spend].
- `cde run fg-status` fails with "fine-grained job status service not initialized" -> not enabled on the vcluster -> rebuild per-task status from each job's run history, or `cde run logs --type <task>/attempt_1` [Collections, Mule].
- Driver exits with ImportError when using `mapInArrow` -> pyarrow missing from the python-env -> pin `pyarrow` in the resource requirements; jobs without arrow/pandas UDFs need none [Spend, Mule].
- A python-env rebuild takes 6-7.5 min, not "1-3 min" -> real dependencies (Great Expectations, pyarrow) -> poll `cde resource describe` until ready; the deploy script exits if the env is not ready [Collections, Spend, CASA].
- Stdlib-only job code needs no python-env -> no resource build at all -> write simple parsers/writers in the standard library where practical [OGX].
- Spark 3.5 streaming fails with "Error parsing '60s' to interval" -> Spark 3.5 wants long-form intervals -> use `"60 seconds"` [OGX].
- Stream restarts must not duplicate data -> foreachBatch replay -> stamp each Iceberg commit with `<query id>:<batch id>` and skip replayed batches [OGX].
- A long-running streaming job is allowed -> the vcluster has no job timeout -> run the producer in `--mode live --duration N` so it stops by itself [OGX].
- "does not support REPLACE TABLE AS SELECT" on gold -> a bare `writeTo().createOrReplace()` goes to the Hive-backed `spark_catalog` and makes a Hive table -> always create through a helper with `using("iceberg")` [GDL, OGX].
- Silver fails at import with an `AssertionError` from `F.lit` -> a Column was built at module load, before a SparkSession existed -> build Columns inside functions; test imports in a fresh process [GDL, OGX].
- Executors cannot import the job module, or `@dataclass` breaks -> modules loaded with `importlib` are pickled by reference once they are in `sys.modules` -> register the module only during `exec_module`, then remove it [Collections, OGX].
- `lru_cache` wrappers cannot be shipped to workers -> cloudpickle pickles them by reference -> use a plain dict memo [Spend].
- `createDataFrame(rdd)` of nested rows took 280 s -> slow row path -> `mapInArrow`, one pass per table: 47-50 s [Spend].
- Gold MERGE does not add a new column -> MERGE `INSERT *` does not evolve the schema -> drop and rebuild the table on a column change [Spend].
- REPLACE left no snapshot parent and churned Atlas GUIDs -> `createOrReplace` -> use `overwritePartitions` per date [Churn, GDL].
- The CDE repository refuses to sync with "non-fast-forward" after a force-push -> the repository only pulls -> point the jobs at a temp repo (`cde job update --mount-1-resource`), recreate the repo, point back [Collections].
- Private repo, no token wanted in CDE -> no credential to store -> upload a files resource built from `git archive` of a pushed commit; public repos use a Git repository resource [CXR, OGX, Mule].
- `cde repository sync` fails with "unable to pull repository: ... authentication required: Repository not found", so `deploy_dag.sh` stops before updating the DAG -> the GitHub repo was made private and the Git repository resource has no credential; jobs keep running from the last synced copy -> make the repo public again, or recreate the repository with `--credential` (a `cde credential create --type basic` read token), or switch to a files resource from `git archive` [Spend].
- `deploy_jobs.sh` leaves orphaned jobs behind -> it creates and updates but never deletes -> delete retired jobs by hand [Collections].
- Kafka brokers are unreachable from the laptop -> port 9093 is closed externally -> create topics and produce from a CDE job (Java AdminClient, Spark Kafka writer over SASL_SSL PLAIN) [OGX].
- Airflow: a manual trigger "succeeded" in 8 s and ran no task -> the DAG `start_date` was after the trigger -> set `start_date` to the most recent schedule time before the deploy [Collections, Churn].
- Airflow: a DAG registers unpaused and fires the latest closed interval at once -> `catchup=False` with a past `start_date` -> set `is_paused_upon_creation=True` and set Variables before unpausing; unpausing runs the closed interval immediately [Spend, Mule, CASA, Collections, Churn].
- Airflow: `cde job create --schedule-paused` is rejected ("does not support setting schedule from outside of DAG") -> CDE Airflow jobs take their schedule from the DAG -> pause in the DAG file [Spend].
- Airflow: `--schedule-enabled false` changes nothing -> "enabled" means a schedule exists; "paused" is the runtime state -> use `cde job schedule pause` and check `paused` [Mule].
- Airflow: `cde job schedule resume` fails (exit 1, prints the usage) -> the CLI's verbs are `pause` / `unpause` (and `pause-all` / `unpause-all`); there is no `resume` -> unpause with `cde job schedule unpause --name <dag-job>`; a `platform.py start` copied from OGX needs this fix [Spend].
- Airflow: `cde job run` of a paused DAG is refused -> CDE blocks manual runs of paused DAGs -> unpause first [Mule, CXR].
- Airflow REST manual trigger returns 403 for the workload user -> RBAC -> trigger with `cde job run --name <dag-job> --config-json` [Spend].
- Airflow: the CAI-trigger task failed on one `ConnectionResetError` poll while the CAI run went on -> a transient drop -> retry polls (up to 10 RequestException/5xx in a row), re-raise 4xx, 90 min deadline, task retries=0 [Churn, CASA, Collections].
- Airflow: a failed batch showed as a successful DAG run -> run state comes from the last tasks, and `reconcile` (`all_done`) succeeded -> add a final `batch_complete` task with `all_success` [GDL, OGX].
- Airflow: overlapping runs clobber the same date -> scheduled runs overlapped -> `max_active_runs=1` [Spend].
- Airflow: a fixed DAG failed again on a run that began before the redeploy -> in-flight runs keep the old DAG -> redeploy, then re-run that date manually; allow about 30 s for re-parse [Spend, Collections].
- Airflow: nightly DAG runs fail about 90 min after the schedule with the first task `failed`, duration 0, empty `hostname`, `queued_when` 30 min before its end, no Spark run in `cde run list`, and `cde run logs --type <task>/attempt_1` says "could not open logs file" -> the task never got a worker slot on the full shared virtual cluster (queued timeout 30 min, then the same on its retry) -> read task instances over the Airflow REST API (`/dags/<id>/dagRuns/<run>/taskInstances`), free the cluster (pause idle schedules, kill stale runs), then backfill the missed dates [CXR].
- Airflow: `cde job schedule resume` fails -> CDE CLI 1.23.0 has `pause` / `unpause` (and `pause-all` / `unpause-all`), no `resume` -> use `cde job schedule unpause --name <job>` [CXR].
- `cde job list` / `cde run list` fail now and then with `Get ".../dex/api/v1/jobs?...": EOF` -> a dropped connection to the CDE API -> retry read-only CLI calls (list, describe) a few times with a back-off; never retry kill, pause or run [CXR].
- Airflow Variables are set over the vcluster's Airflow REST API with a Knox token for the workload user -> scripted -> touch only your own prefix's keys [Spend, CXR, OGX, Mule].

## CDW Impala/Hive
- Syntax errors on `rows`, `method`, `format`, `change`, `matched` -> Impala reserved words (409; `method` since 3.0) that Spark accepts -> backtick-quote every generated identifier, rename columns, and test aliases against the list [Collections, Spend, GDL, OGX, Mule].
- "TIMESTAMPTZ column that Impala cannot write" -> Impala 4.5 cannot write Iceberg timestamptz -> use `timestamp_ntz` holding UTC (Spark in UTC) for tables Impala writes [GDL, OGX].
- Clash on column `_file` -> Iceberg reserves `_file` as a metadata column -> name the lineage column `_source_file`; `split` was renamed too [CXR, OGX].
- `CREATE OR REPLACE VIEW` is not supported -> Impala has no such statement -> DROP, then CREATE [Churn].
- A `LIKE 'manual__%'` filter matched `manual-2026-...` -> `_` is a LIKE wildcard -> escape the underscore, or test the specific pattern first [Collections, Mule].
- Numeric values come back as strings or Decimal through impyla -> blanket `str()` / the Decimal type -> stringify only ids and dates; convert Decimal to float [Mule, Churn, Spend].
- Each laptop query costs 2-10 s and `INSERT ... VALUES` of 30k rows takes about 9 min -> HTTP round trips to the warehouse -> read one customer at a time, insert in 500-5,000-row chunks, run heavy publishes from CAI [Churn, Spend].
- Run-meta row visible before the list was complete -> the run row was written first -> write the run/manifest row last [Churn, Spend].
- The endpoint picked the wrong run -> backfills write older dates later -> select by latest run date (published), not latest write [Collections].
- An Impala `DESCRIBE HISTORY` hung for 13 min while the vcluster was starved -> shared-environment pressure -> retry later; pin reads with `FOR SYSTEM_VERSION AS OF` [Spend].
- The CDW instance's own connection uses a cluster-internal host (port 28000) that CAI cannot reach -> internal endpoint -> use the warehouse's public endpoint (443, HTTP transport, `cliservice`, LDAP) [Churn, Collections].
- Ranger custom mask `TRUNC({col}, 'YYYY')` fails on INT columns (`trunc(INT, STRING)`) -> a tag propagated to a derived INT column -> see SDX (propagate=false) [Churn].

## Data Visualization
- API returns 401 with a password -> the instance signs in with SAML -> use a Data Visualization API key [GDL, OGX].
- Export refused ("Manage dashboards role is required") through the CAI proxy -> the user's role is assigned on first browser login -> open the app once in a browser; then `vizcml-rw` grants System Admin [Churn, Mule].
- Imported dashboards land in the Private workspace -> import ignores the file's workspace (`workspace_update: false`) -> send form field `workspace=<name>`, or move visuals with POST `/arc/adminapi/v1/visuals/<id>` (POST without an id creates a copy) [Collections, CASA, Spend].
- The admin API lists dashboards only -> a dashboard's visuals move with it -> move the dashboard [Spend].
- PK collisions between projects on a shared instance -> overlapping export PK ranges -> give each project a fixed PK range (9000+, 12000+, 13000+); re-imports match by UUID [OGX, CXR, GDL].
- Changes appear up to 30 min late, and `--verify` reads stale results -> connection result cache `CACHE.RETENTION` 1800 s -> wait for expiry, or verify after the cache window [CASA, Spend].
- Data API returned ParseException on `[near_miss]` -> new view columns were treated as literals until the dataset was re-imported -> re-import the datasets after changing views [Churn].
- A table sorted by a measure came back unordered from the Data API (top 15) -> sort is not honoured -> add a rank column in the view and filter `rank <= N` [Mule].
- The instance has no URL in the CDW API -> discovery gap -> keep the instance URL in config/.env [Spend].
- A separate Data Visualization app can be launched in CAI (runtime 8.1.7, about 1.5-4.5 min to start) -> option alongside the CDW instance -> build its connection with the public Impala endpoint [Churn, Mule].

## Kafka/Schema Registry/SMM
- Broker port 9093 is closed from the laptop (public IPs notwithstanding) -> network policy -> run admin and producer work from CDE, or create topics through the SMM REST API over Knox [OGX].
- Authentication: SASL_SSL PLAIN with the CDP workload user and password; truststore = the environment root CA (`cdp environments get-root-certificate`), kept outside Git [OGX].
- Schema Registry and SMM are reached through the Data Hub's Knox `cdp-proxy-api` with basic auth [OGX].
- Routed copies would be re-ingested -> the bronze stream subscribes to every configured topic -> keep derived topics (`ogx.alarm.priority`) out of the stream's topic list [OGX].

## NiFi
- The Schema Registry controller property is `Schema Registry URL` (singular), through Knox with basic auth -> validate every property against the component definitions before creating anything [OGX].
- `CdpCredentialsProviderControllerService` needs CDP access keys -> no keys wanted in the flow -> use `PutCDPObjectStore` with a Kerberos user service [OGX].
- Kafka: `Kafka3ConnectionService` (SASL_SSL PLAIN), referencing the existing root truststore SSL service without modifying it; version the flow in a NiFi Registry bucket and export it without sensitive values [OGX].

## Flink/SSB
- No live Flink/SSB runs: `federal-flink` exists, but streaming was built on CDE Spark Structured Streaming; SSB kept as the documented option for sub-second alerting [OGX].

## SDX (Atlas, Ranger)
- Atlas searches return nothing -> Iceberg tables are `iceberg_table` / `iceberg_column`, views are `hive_table` / `hive_column` -> search both types [GDL, OGX].
- Atlas has no Spark lineage for a table -> the Spark Atlas hook drops outputs on a "Detected cycle" (a lookup reads the table being written) -> checkpoint only that lookup [GDL].
- Ranger built-in "Date: show only year" mask left a DATE column in clear -> it does not apply on Impala DATE -> custom mask `TRUNC({col}, 'YYYY')` [Churn].
- The custom mask broke queries on a derived INT column -> the classification propagated from `date_of_birth` to `age` -> set propagate=false on the source tag and tag the target column directly; tag sync picks it up within a minute [Churn].
- Masking gaps (address lines, whole-record payload/text/binary columns left clear) -> the name map missed them -> add them to the tag map, REDACT the text columns, use MASK_NULL for binary; profiler tag rules help find them [GDL].
- Data Catalog profilers never ran -> the shared compute cluster's infra node group was at 0 nodes -> scale it (to 3); profilers then run hourly [GDL].
- Atlas and Ranger REST go through the data lake Knox gateway with basic auth; the tag service `cm_tag` is linked to `cm_hive`; check as masked and unmasked users in Hue [GDL, OGX, Churn].

## Git/CI/secrets
- GitHub-hosted runners fail with `ConnectTimeout` -> the workbench resolves to a private IP -> use a self-hosted runner (`runs-on` from repo variable `CAI_RUNS_ON`), or a publicly reachable workbench (federal works) [CXR, Spend].
- A workflow skipped the CAI job after the secrets were set -> a run keeps the secrets it started with -> re-run the job [Spend].
- Workflows go red before secrets exist -> no `CAI_URL` yet -> print a notice and succeed until `CAI_URL` is set [CXR, OGX, Spend].
- GitHub secrets target one workbench only -> other workbenches sync by a manual run of the sync job [CXR].
- A CI dispatch sat in scheduling for its whole 1 h timeout -> the CAI quota was taken by the running app -> stop the app before CI chains [CXR].
- The sync job could reset code under running DAG tasks -> concurrent runs -> concurrency group, plus an `EXPECTED_GIT_SHA` check that stops the chain on a newer push [Spend, CXR].
- Old access key gives "Environment … not found" -> the new environment is in another CDP account -> `cde` CLI with workload user/password (`auth-pass-file`) [Spend].
- Rewriting history (removing trailers) breaks consumers that only pull -> CDE repository non-fast-forward -> see CDE repository recreate [Collections].
- Code pushed to CAI with the files API leaves the project's git HEAD stale, and model versions carry the old SHA -> run the sync job when possible [OGX].
- Secrets appeared in screenshots, and in run environment dumps -> plaintext env in UI and API -> check repo visibility; redact and re-check images before committing; gitleaks pre-commit hook; `.gitignore` for `.env`, `*.pem`, `*.jks`, `*.keytab` [Mule, OGX].
- Background `nohup` pollers died without output (one stopped while the laptop slept, others as soon as the tool call returned) -> the agent shell does not keep detached children alive reliably -> poll in the foreground with a bounded loop; after any gap, read live state from the API [OGX].
- `python scripts/run_local.py` fails on Linux (CI, CAI jobs) with `module 'platform' has no attribute 'system'` from `import uuid`, yet works on the laptop -> `scripts/platform.py` shadows the standard library's `platform` for every script run from `scripts/` (it is first on `sys.path`); macOS loads `platform` at start-up and hides it -> never name a module after a stdlib one (`scripts/platform_ctl.py`); test `scripts/*.py` stems against `sys.stdlib_module_names` [CXR; OGX has the same latent clash].
- `status` used as a loop variable fails in zsh polling scripts -> `status` is read-only in zsh -> use another name (`st`) [Collections].
- Hugging Face anonymous downloads hit 429 -> rate limit -> use a read token held as a project variable; Kaggle now issues `KGAT_` tokens via `KAGGLE_API_TOKEN` (no `kaggle.json`); an HF mirror pinned to a revision removes the need for Kaggle [OGX, CXR].

## Data/ML method
- "AutoImageProcessor requires the Torchvision library" -> transformers 5.x image processors -> add `torchvision` [CXR].
- Spark test workers ran the system Python 3.9 -> `PYSPARK_PYTHON` was unset -> set `PYSPARK_PYTHON=sys.executable` [Churn].
- pip resolved pandas 3 -> sibling code assumes pandas 2 -> pin `pandas>=2.2,<3` [Spend].
- TabICL silently used MPS on a Mac -> the device was left empty -> set the model device explicitly [Churn].
- A gate fails at a small validation set (about 20 positives) -> the 95%-sensitivity threshold is unstable -> scale up, or set the threshold from out-of-fold scores; the drift check needs at least 30 samples (`TOO_FEW`) [CXR, OGX].
- Thresholds or C picked on TEST -> optimistic numbers -> record it in the log and runbook [CXR].
- An operating threshold above the P1 band cut-off -> bands start at `max(p1, threshold)` [CXR].
- A constant feature's std is about 1e-16, not 0, giving NaN distances -> treat scale <= 1e-6 as 1 [Spend].
- `prior_correct` returned 1.0000000000000002 -> clip probabilities [Churn].
- Fixed rule thresholds did not match the generated data (stuck-sensor minimum count, one pressure limit for all tags, severity area) -> calibrate rules against the actual distributions; per-tag overrides [OGX].
- The generator planted patterns on a different sample each day -> no pattern accumulated -> fix the sample at the first date [GDL].
- Prefix-stable generators (a later as-of only adds rows) let federal reproduce go01 and laptop numbers exactly -> use them as a cross-environment check [CASA, Collections, Churn, Spend].
- Larger cohorts made seasonal naive stronger (WAPE margin 0.125 -> 0.028) -> re-check gate margins at cluster scale [Spend].
- PSI drift alerts every night by design (demo frames differ from training) -> limit drift-only retraining to weekly and report drift, do not tune it away [OGX, CXR].
