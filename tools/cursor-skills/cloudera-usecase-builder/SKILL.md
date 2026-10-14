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

Write a `docs/REUSE_INVENTORY.md` row for every file copied: source, target, as-is or adapted.

## Workflow

Copy this checklist into the chat and keep it current:

```
- [ ] 0 Discovery: use case, decision supported, KPIs, data (synthetic + one pinned public set), services
- [ ] 1 PLAN.md: story, requirements map, platform mapping, names, phases, approval list -> user approves
- [ ] 2 Scaffold: layout, config/, contracts/, .env.example, .gitignore, hooks, tests, local runner
- [ ] 3 Generators + landing (manifests, planted faults)
- [ ] 4 Bronze / quarantine / load_audit, recon at bronze
- [ ] 5 Streaming (if any): producer, topics + schemas, CDE streaming jobs, stream recon
- [ ] 6 Silver, master data, gold (SCD2), recon per layer
- [ ] 7 CDE live: repository/resource, jobs, Airflow DAG, one date end to end
- [ ] 8 CDW semantic views + KPI consistency check; Data Visualization dashboards as code
- [ ] 9 CAI: project, env, jobs chain 00-04 (+05 drift, 08 retrain), model endpoint, AI Registry
- [ ] 10 Guardrails, MLOps loop, app (Streamlit) on CAI
- [ ] 11 Governance: Atlas classifications + glossary, Ranger tag masking
- [ ] 12 Ops: scripts/platform.py stop/start, runbooks, README status
```

Rules for every phase:
1. One commit per phase, `pytest -q` green first; push at each milestone.
2. Local first (Spark 4 + Iceberg + SQLite catalog, Kafka on localhost), then live.
3. Live: one service at a time; read its status back; record run ids, counts and every plan
   change in `docs/PROJECT_LOG.md` (newest last, no secret values).
4. Present ONE approval list up front (pushes, CAI project/jobs/models/app, CDE resources/jobs/DAG,
   Kafka topics/schemas, first CDW write, Atlas/Ranger, Data Viz import, dataset download). After
   approval, proceed; anything destructive, outside the prefix, or on shared services asks again.

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
- Long live waits: run in the background writing to a log file and poll it; never block a tool
  call for many minutes, and never pipe a long-running command through `cut`/`grep` (it buffers).

## Platform essentials (details and every known failure in reference.md)

- CAI on PBJ runtimes: the model function MUST be decorated with `cml.models_v1.cml_model`,
  otherwise every call hangs ("model busy").
- CAI capacity: jobs, app and models at 1 vCPU / 4 GB schedule at once; 2 vCPU / 8 GB may queue.
- Register the model version in the AI Registry BEFORE the endpoint deploy, so a slow deploy
  never loses the version. Registry API path: `/api/v2/registry/models`.
- Streamlit app: launcher supervises the child (restart on exit); per-thread Impala connection
  that reconnects; cache expensive scoring with `st.cache_data`.
- CDE: public repo -> Git repository resource; private repo -> files resource or a credential.
  Load stage modules unregistered so executors pickle closures by value.
- Data Hubs, CDW, CDE and CAI are shared by sibling demos: stop/start them only with consent.

## Additional resources

- Every platform failure seen so far, cause and fix: [reference.md](reference.md)
- Repo layout, config skeletons, PLAN.md and PROJECT_LOG.md templates, approval list: [templates.md](templates.md)
