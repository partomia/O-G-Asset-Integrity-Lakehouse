# Templates

## Repository layout

Keep only the folders the use case needs; names are fixed so tooling and docs carry over.

```
<Repo>/
├── README.md  PLAN.md  .env.example  .gitignore  common.py  cdsw-build.sh
├── requirements.txt  requirements-ci.txt  requirements-model.txt   (model: slim, pinned sklearn)
├── .github/workflows/   ci.yml (pytest + offline chain)  cai-mlops.yml (GitHub -> CAI API v2)
├── config/              lakehouse.json  pipeline.yaml  streaming.json  kpi.json  governance.json
│                        guardrails.yaml  platform.json  ci.yaml  expected_recon.json  consumers.json
├── contracts/           products/<name>.json  objects/<format>.json  stream/<topic>.json
├── generators/          seeded synthetic sources + planted faults + manifests
├── stream/              producer/ (seeded, rate-controlled, control messages)  schemas/*.avsc
├── extract/             format parsers shared by CDE jobs and tests (stdlib where possible)
├── cde/jobs/            land, bronze, extract, silver, <mdm>, gold, recon, run_stages, stream_*
├── cde/dags/            <xx>_dag.py (CDEJobRunOperator per stage, recon all_done, batch_complete)
├── cde/scripts/         deploy_jobs.sh  deploy_dag.sh  deploy_streaming.sh
├── mlcore/              gate, registry, deploy, guardrails, hashlock, split (shared ML spine)
├── usecases/            uc01_<name>/ config.yaml  build_features.py  train.py  score.py  app_page.py  media_page.py
│   └── _template/       scaffold for uc02+
├── app/                 launch_app.py  app.py  discover.py  vision_app.py (optional unstructured app)
├── ci/                  setup_cai.py  cai_jobs.py  push_files.py  sync_code.py  project_stop.py  trigger_cai_pipeline.py
├── sql/semantic/        10_kpi_*.sql  20_mis_views.sql  50_dashboard_views.sql
├── dataviz/             build_dashboard.py  <xx>_dashboards.json
├── nifi/                deploy_flow.py  flows/<flow>.json (exported, no sensitive values)
├── scripts/             run_local.py  run_semantic.py  governance.py  platform_ctl.py  install_hooks.sh  hooks/
├── docs/                PROJECT_LOG.md  DEMO_RUNBOOK.md  REUSE_INVENTORY.md  + one per capability
└── tests/
```

`.gitignore`: `.env`, `.env.*` (keep `.env.example`), `data/`, `feature_store/`, `models/archive/`,
`outputs/`, `checkpoints/`, `mlruns/`, `*.jks`, `*.keytab`, `*.pem`, `.venv/`, `.cursor/`. CAI model
builds also skip git-ignored files: the champion folder the endpoint loads must reach the build
(written there by the deploy job inside CAI, or not ignored).

## PLAN.md skeleton

```markdown
<Use case> — Build Plan
<date> · @<owner>
Overview            the one claim the demo proves; the story; the decision supported (and what the
                    model does NOT decide); scale (entities, days, rates)
End-to-end flow     sources -> CDE -> CDW/CDV, stream -> CDE streaming, gold -> CAI -> app
Requirements map    # | requirement | where it is met (file or service)
Platform mapping    layer | service | what runs there
Names and conventions   (see SKILL.md conventions, filled in for <xx>)
Synthetic sources   source | type | format | generator | cadence ; planted faults
Layers and model    bronze / silver / master / gold tables; KPIs (certified definitions)
ML                  heads, features (one definition + hash), threshold rule, gate KPIs, registry
Guardrails          layer | guardrail | code | what happens when it fires
Application         pages and what each shows
Phases              ☐ 0..12 with M1/M2/M3 column; one commit each; PROJECT_LOG after every live step
Resources           defaults, ceilings, triggers (section 18); log changes in platform.json + PROJECT_LOG
How we work         copy-then-adapt, credentials, one approval list, never touch siblings
Decisions / open items
Milestones          M1 demo-ready | M2 production (streams, WAP, field head) | M3 extensibility + docs
```

## PROJECT_LOG.md entry

```markdown
## <YYYY-MM-DD HH:MM> - <what>

- **<Service> <object>** `<name>` (`<id>`): what ran (run ids), result with counts, duration.
- **Plan change: <topic>.** What the plan said, what was found, what was done instead and why.
- Open item: <what is still wrong>, <what is needed> (approval, access, platform fix).
```

## Approval list (present once, before any live action)

```
Git:   push to <remote> (public|private)
CAI:   project <name> from <git url>; project env vars (names only); jobs <xx>-00..08 at 1 vCPU / 4 GB;
       model <name> (1 vCPU / 2 GB); application <name> (<cpu> / <mem>); AI Registry model(s) <names>
CDE:   resource <name> (git|files); jobs rsingh-<xx>-*; DAG job rsingh-<xx>-orchestration;
       streaming jobs rsingh-<xx>-stream-*; driver/executor sizes
Kafka: topics <names> (partitions, retention); schemas in Schema Registry
NiFi:  process group <name>; registry bucket <name>
CDW:   databases rsingh_<xx>_*; first write; semantic views
CDV:   connection; datasets/dashboards with PKs <range>
SDX:   classifications <XX>_SENSITIVE_*; glossary; Ranger policies rsingh-<xx>-* (users)
Data:  public dataset <id>@<revision> (licence)
Ops:   scripts/platform_ctl.py stop/start at project scope (shared services need --scope platform --yes-shared)
Data:  public dataset <id>@<revision> (licence); WAP spike table (drop after doc)
Ceilings: CDE VC to 32 CPU / 128 GiB; CAI job to 2/8; app to 4/16; Impala autoscale +1 group; Kafka partitions 2×
       (document exact numbers in PLAN section 18; never exceed without a new approval)
```

## config/platform.json

```json
{
  "environment": "<cdp-env-name>",
  "project": {
    "cai_project": "rsingh-<xx>-...", "cai_job_prefix": "<xx>-", "cai_apps": ["<app name>"],
    "cai_models": ["<model>"], "cai_scheduled_jobs": {"<xx>-05-nightly-drift": "0 2 * * *"},
    "cde_job_prefix": "rsingh-<xx>-", "cde_streaming_jobs": [], "cde_dag_jobs": ["rsingh-<xx>-orchestration"],
    "nifi_process_groups": []
  },
  "platform": {
    "datahubs": {"kafka": "<dh>", "nifi": "<dh>", "flink": "<dh>"},
    "cai_workspace": "<ws>", "cde_service": "<svc>", "cde_vc": "<vc>", "cdw_vws": ["<impala vw>"], "cdw_dbc": null
  },
  "stop_order": ["cai", "nifi", "cde", "cdw", "flink", "kafka"],
  "start_order": ["kafka", "flink", "cdw", "cde", "nifi", "cai"],
  "resources": {
    "defaults": {"cde_batch": {"executor_cores": 8, "executor_memory_gb": 32}, "cai_app": {"cpu": 2, "memory_gb": 8}},
    "ceilings": {"cde_vc_quota_cpu": 32, "cai_app": {"cpu": 4, "memory_gb": 16}},
    "changes": []
  }
}
```

Discover the platform names read-only: `cdp datahub list-clusters`, `cdp ml list-workspaces`,
`cdp de list-services` + `cdp de list-vcs --cluster-id`, `cdp dw list-clusters` + `cdp dw list-vws`.
