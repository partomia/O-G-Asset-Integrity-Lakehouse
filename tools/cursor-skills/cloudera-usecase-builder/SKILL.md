---
name: cloudera-usecase-builder
description: >-
  Builds an end-to-end Cloudera CDP demo use case (synthetic sources, Iceberg medallion on CDE
  Spark and Airflow, Kafka/NiFi streaming, CDW Impala semantic layer, Data Visualization
  dashboards as code, CAI ML chain with KPI gate, AI Registry and Streamlit app, SDX governance,
  GitHub CI) in the house style of the py_aws sibling repos. Use when starting a new Cloudera
  use-case repo, planning one (PLAN.md), adding a layer to one, deploying it live, or when the
  user mentions CDE, CAI, CDW, Data Visualization, NiFi, Kafka, Iceberg lakehouse demo.
disable-model-invocation: true
---

# Cloudera use-case builder

One repo per use case, built in phases, verified live one service at a time, every live action
recorded in `docs/PROJECT_LOG.md`. Copy from a sibling repo first, then adapt; never edit siblings.

## Reference implementations (read before writing the counterpart)

All under `/Users/rsingh/Downloads/py_aws/`:

| Need | Copy from |
|------|-----------|
| Lakehouse spine: generators, landing + manifest, bronze/silver/MDM/gold, SCD2, recon, KPI views, governance | `General-DataLakehouse` (GDL) |
| ML spine: one feature definition + hash, train/gate/deploy chain, AI Registry, silent trial, Streamlit app, CI to CAI | `Chest-X-ray-Triage` (CXR) |
| Unstructured objects, Kafka producer, CDE streaming, NiFi flow as code, guardrails, platform stop/start, Airflow DAG | `O-G-Asset-Integrity-Lakehouse` (OGX) |
| Tabular ML on gold (banking) | `Customer-Churn-Prediction`, `Collections-Delinquency-Roll-Forward-Prediction`, `Mule-Account-Identifier`, `ALM-IRRBB-CASA-Behavioural-Forecasting`, `Spend-Analytics` |
| **Production-prototype lakehouse backbone**: milestones M1–M3, data products, core/use-case split, config-driven DAG and dashboards, PIT features, batch-grouped splits, planted-cause truth, two Streamlit apps (structured + unstructured), `platform_ctl` + resources ceilings | `EV-Factory-to-Field-Lakehouse` (EVX) |

Write a `docs/REUSE_INVENTORY.md` row for every file copied: source, target, as-is or adapted.

## Entry modes

1. **New lakehouse** — full repo from Step 1 of the customer prompt: `PLAN.md` with milestones,
   one approval list (M1–M3), WAP spike in phase 0, generators through gold, first use case inside
   `usecases/uc01_*` from day one (not a throwaway script folder).
2. **New use case on an existing lakehouse** — follow the host repo's `docs/ADDING_A_USE_CASE.md`:
   scaffold from `usecases/_template/`, readiness SQL, feature view, baseline job; prove with
   `git diff` that no core path changed. Do not move uc01 into `mlcore/` retroactively.

## Workflow

Copy this checklist into the chat and keep it current:

```
- [ ] 0 Discovery: shared-capacity read-only; WAP spike; pin datasets/models; generator dry run at CI scale; freeze counts
- [ ] 1 PLAN.md: story, requirements map, platform mapping, names, phases with M1/M2/M3 tags, approval list with ceilings -> user approves once
- [ ] 2 Scaffold: layout, config/ (incl. platform.json resources), contracts/, .env.example, hooks, tests, run_local.py, platform_ctl.py
- [ ] 3 Generators + landing (manifests, planted faults)
- [ ] 4 Bronze / quarantine / load_audit, recon at bronze
- [ ] 5 Streaming (if any): producer, topics + schemas, CDE streaming jobs, stream recon
- [ ] 6 Silver, master data, gold (SCD2), recon per layer
- [ ] 7 CDE live: repository/resource, jobs, Airflow DAG, one date end to end
- [ ] 8 CDW semantic views + KPI consistency check; Data Visualization dashboards as code
- [ ] 9 CAI: project, env, jobs chain 00-04 (+05 drift, 08 retrain), model endpoint, AI Registry
- [ ] 10 Guardrails, MLOps loop, app (Streamlit) on CAI; optional second app for unstructured/media pages
- [ ] 11 Governance: Atlas classifications + glossary, Ranger tag masking
- [ ] 12 Ops: platform_ctl stop/start, DEMO_RUNBOOK.md, walkthrough check, `.cursor/rules/project-start-stop.mdc`
      (`.gitignore`: `.cursor/*` + `!.cursor/rules/`)
M1 done when: default-scale data, champion gated and deployed, DAG task for scoring, 3+ dashboards,
workbench demo, platform_ctl stop leaves nothing of the prefix running. M2/M3: see host PLAN.md.
```

Rules for every phase:
1. One commit per phase, `pytest -q` green first; push at each milestone.
2. Local first (Spark 4 + Iceberg + SQLite catalog, Kafka on localhost), then live.
3. Live: one service at a time; read its status back; record run ids, counts and every plan
   change in `docs/PROJECT_LOG.md` (newest last, no secret values).
4. Present ONE approval list up front for the whole project (M1–M3), including resource ceilings
   (CDE VC quota, CAI instance group max, Impala autoscale), scoped destructive ops on the project
   prefix only, model/dataset downloads, and skill edits. After approval, proceed; anything outside
   the prefix or on shared services asks again. Log every capacity increase in `config/platform.json`
   `resources.changes` and `docs/PROJECT_LOG.md`; scale back at end of session unless told to keep.
5. Keep this skill current: when a platform failure is solved, add a symptom -> cause -> fix
   bullet to reference.md (and a line under Platform essentials if it changes how projects are
   built) in `O-G-Asset-Integrity-Lakehouse/tools/cursor-skills/cloudera-usecase-builder/`
   (the installed skill is a symlink to it), then commit and push that repo.

## Conventions

- Short code `<xx>` (gdl, cxr, ogx ...). Databases `rsingh_<xx>_{bronze,silver,<mdm>,gold,semantic,ref}`;
  CDE `rsingh-<xx>-*`; CAI jobs `<xx>-00-sync-code` .. `<xx>-08-*`; env vars `<XX>_*`;
  Data Viz objects `<XX> ...` with fixed primary keys in a free range (check siblings first);
  Atlas `<XX>_SENSITIVE_*`; Ranger `rsingh-<xx>-*`; landing `s3a://<bucket>/data/IB/rsingh_<xx>/landing/<source>/<date>/`.
- Config is data: `config/lakehouse.json`, `pipeline.yaml` (ML), `streaming.json`, `kpi.json`,
  `governance.json`, `guardrails.yaml`, `platform.json`, `ci.yaml` (offline overlay).
- Credentials only in git-ignored `.env` (taken from siblings' `.env`, re-prefixed); `.env.example`
  has placeholders; gitleaks pre-commit + commit-msg hook (no attribution trailers).
- Never print environment variables, job `environment` fields or `.env`: filter output.
- Long live waits: a bounded foreground poll loop (status every 15 s, explicit timeout) is the
  reliable form. Background `nohup` jobs can be killed when the tool call returns or the laptop
  sleeps, so re-read live state from the API rather than trusting a stale log. Never pipe a
  long-running command through `cut`/`grep` (it buffers).

## Platform essentials (details and every known failure in reference.md)

- CAI on PBJ runtimes: the model function MUST be decorated with `cml.models_v1.cml_model`,
  otherwise every call hangs ("model busy").
- CAI model builds copy every project file that is not git-ignored: `.gitignore` must list the
  workbench home state (`.local/ .cache/ .config/ .ipython/ .jupyter/ .experiments/`) or builds
  hang in "pushing", and must NOT list the serving model folder (exclude it from Git with
  `.git/info/exclude` instead). Builds still stall now and then: time out a build after ~10 min
  and start a new one rather than waiting; make registry registration idempotent per MLflow run.
- CAI capacity: jobs, app and models at 1 vCPU / 4 GB schedule at once; 2 vCPU / 8 GB may queue.
- Register the model version in the AI Registry BEFORE the endpoint deploy, so a slow deploy
  never loses the version. Registry API path: `/api/v2/registry/models`.
- Streamlit app: launcher supervises the child (restart on exit); per-thread Impala connection
  that reconnects; cache expensive scoring with `st.cache_data`.
- CDE: public repo -> Git repository resource; private repo -> files resource or a credential.
  Load stage modules unregistered so executors pickle closures by value.
- CDE Airflow DAGs: `max_active_runs=1`, `dagrun_timeout` and a per-task `execution_timeout`.
  The virtual cluster quota (20 CPU / 80 GB) is shared, and runs stuck "running" for days block
  every other project's stages in "starting".
- Data Hubs, CDW, CDE and CAI are shared by sibling demos: stop/start them only with consent.
  Every project gets `scripts/platform_ctl.py` + `config/platform.json` (status/stop/start, project
  scope by default) so idle demos release their share. Port it from OGX's or EVX's
  `scripts/platform_ctl.py` (never name a script after a stdlib module). CAI schedules stop by
  clearing the cron (API v2 ignores `paused`); CDE schedules use `pause` / `unpause`. CAI self-stop:
  job `stop-<xx>-project` running `ci/project_stop.py`.
- **Model choice:** tabular scoring — LightGBM / logistic / histogram GB; winner by grouped OOF
  PR-AUC; isotonic calibration. Never an LLM for scores or automated actions. Text summaries only
  (M3): small CPU GGUF model with grounded citations and a guardrail test.
- **Resource rule (14):** add shared capacity before enlarging a single job; never resize a CAI
  instance group while engines run; 2 vCPU / 8 GB jobs queue longer than 1 / 4 on a full quota.

## Additional resources

- Every platform failure seen so far, cause and fix: [reference.md](reference.md)
- Repo layout, config skeletons, PLAN.md and PROJECT_LOG.md templates, approval list: [templates.md](templates.md)
- Lakehouse backbone pattern (data products, use-case boundary, PIT, WAP, freshness): [lakehouse-backbone.md](lakehouse-backbone.md)
