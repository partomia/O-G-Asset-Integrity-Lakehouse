# Use-case playbook: building the next Cloudera demo fast

Eight repos under `~/Downloads/py_aws/` (General-DataLakehouse, Chest-X-ray-Triage, this one, and
five banking ML demos) use the same platform, the same repository shape and the same way of
working. The Cursor skill `cloudera-usecase-builder` captures that so the next use case starts
from it instead of from scratch.

## What the skill is

`tools/cursor-skills/cloudera-usecase-builder/` in this repo (versioned here, installed per user):

| File | Holds |
|------|-------|
| `SKILL.md` | the workflow (phases 0 to 12), conventions, which sibling to copy each part from, platform essentials |
| `reference.md` | every platform failure met so far across the eight repos: symptom, cause, fix, by service |
| `templates.md` | repo layout, PLAN.md skeleton, PROJECT_LOG entry, approval list, `config/platform.json` |

Install (once per machine; a symlink keeps it current with this repo):

```bash
mkdir -p ~/.cursor/skills
ln -sfn ~/Downloads/py_aws/O-G-Asset-Integrity-Lakehouse/tools/cursor-skills/cloudera-usecase-builder \
        ~/.cursor/skills/cloudera-usecase-builder
```

The skill loads only when named (`disable-model-invocation: true`), so it costs nothing in other chats.

## Starting a new use case

1. Create the empty repo folder under `~/Downloads/py_aws/`, open it in Cursor, and add the siblings
   you want to copy from to the workspace (read-only).
2. Prompt, naming the skill:

   > Use the cloudera-usecase-builder skill. New use case: <one paragraph: industry, decision to
   > support, data (structured, unstructured, streaming), ML target, KPIs>. Short code <xx>.
   > Write PLAN.md and the approval list; do not touch Cloudera until I approve.

3. Review PLAN.md (requirements map, names, phases, open items) and the approval list; answer the
   open items; approve once.
4. Let the agent run the phases. Each ends with `pytest -q` green, a commit and a push, and live
   steps end with a `docs/PROJECT_LOG.md` entry. Ask for status at any time with
   `python scripts/platform.py status`.
5. Before a demo: `python scripts/platform.py start`, then the runbook (`docs/DEMO_RUNBOOK.md`).
   After it: `python scripts/platform.py stop` (project scope; leaves shared services alone).

## Where the time goes, and how the skill cuts it

| Usually slow | What the skill does instead |
|--------------|-----------------------------|
| Re-discovering platform quirks (PBJ decorator, CAI capacity, Impala reserved words, Spark interval syntax, Knox paths) | `reference.md` lists them with the fix, so the first deploy is the working one |
| Writing connection code (Impala, CAI API v2, CDE CLI, Data Viz, NiFi, Schema Registry) | points at the sibling file to copy for each |
| Long blocking waits on builds and queued jobs | runs live waits in the background and polls a log |
| Approvals one action at a time | one approval list up front, by service |
| Losing track of what ran live | `PROJECT_LOG.md` entry per live step, plan changes called out |
| Leaving clusters running between demos | `scripts/platform.py stop/start` per project, platform scope only with consent |

## Keeping the skill current

When a build hits a new platform issue, add one bullet to `reference.md` (symptom -> cause -> fix,
repo name) in the same commit as the fix. When a new pattern proves itself (for example the
platform stop/start script), add it to `templates.md` and the reference table in `SKILL.md`.

## Stopping and starting a demo

`scripts/platform.py` (config in `config/platform.json`):

| Command | Effect |
|---------|--------|
| `status [--scope platform]` | app, model, schedules and runs on CAI; CDE runs; NiFi group; with platform scope also Data Hubs, CDW warehouse, CDE virtual cluster, CAI workspace |
| `stop` | stops the CAI app and model deployment, pauses the scheduled jobs and stops running ones, pauses `rsingh-<xx>-*` CDE Airflow schedules and kills their running or starting runs (streaming resumes from checkpoints), stops the NiFi process group |
| `start` | resumes CAI and CDE schedules, restarts the newest model deployment and the app, starts the streaming jobs, starts the NiFi group (the "Hold failures" processor stays stopped) |
| `stop/start --scope platform --yes-shared` | the above plus Data Hubs (Kafka, NiFi, Flink), the CDW Impala warehouse, the CDE virtual cluster and the CAI workspace, in dependency order |
| `--only cai,cde` / `--dry-run` | limit layers / print without acting |

Data Visualization has no suspend command in the CDP CLI and is left running. The shared services
are also used by the sibling demos, so platform scope is for the end of a demo season, not a day.

### Adding the script to an existing project

Paste into a Cursor agent opened on that repo:

```text
Add a stop/start/status script for this project's live Cloudera resources, ported from
/Users/rsingh/Downloads/py_aws/O-G-Asset-Integrity-Lakehouse (read scripts/platform.py,
config/platform.json, the "Stopping and starting a demo" section of docs/USE_CASE_PLAYBOOK.md,
and the cloudera-usecase-builder skill). Do not edit that repo or any other sibling repo.

1. Inventory first, from this repo's code and docs/PROJECT_LOG.md, then confirm live (read-only):
   the CAI project name, its apps, model deployments and scheduled jobs; the CDE job prefix,
   Airflow DAG jobs with schedules, and any streaming jobs; NiFi process groups; Kafka topics;
   CDW virtual warehouses and Data Visualization instances used. Show me the inventory.
2. Write config/platform.json in the same shape (project section = only this project's
   resources; platform section = the shared Data Hubs, CDW VW, CDE VC, CAI workspace; stop_order
   and start_order). Drop layers this project does not use.
3. Write scripts/platform.py: `status | stop | start`, `--scope project|platform` (project is the
   default), `--only`, `--dry-run`, and `--yes-shared` required for platform scope.
   Project stop: stop CAI apps and model deployments, pause CAI job schedules and stop active
   runs; pause the project's CDE Airflow schedules (`cde job schedule pause`) and kill its CDE
   runs in "running" or "starting", Spark children before orchestration runs; stop NiFi groups.
   Project start reverses this (resume schedules, restart newest deployment and app, restart
   streaming from checkpoints). Every action filters strictly by this project's prefixes.
   Credentials come from the git-ignored .env only; never print environment values.
4. Make sure every Airflow DAG has max_active_runs=1, dagrun_timeout and a per-task
   execution_timeout; redeploy a DAG only when it has no run in flight.
5. Verify live with `status` and `status --scope platform`, and `stop --dry-run` /
   `start --dry-run`. Do not run a real stop or start without asking me.
6. Add a short README section and a docs/PROJECT_LOG.md entry; run tests; commit and push.
   Add any new platform lesson to the cloudera-usecase-builder skill's reference.md.
```
