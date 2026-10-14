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
| `stop` | stops the CAI app and model deployment, pauses the scheduled jobs and stops running ones, kills running `rsingh-<xx>-*` CDE runs (streaming resumes from checkpoints), stops the NiFi process group |
| `start` | resumes schedules, restarts the newest model deployment and the app, starts the streaming jobs, starts the NiFi group (the "Hold failures" processor stays stopped) |
| `stop/start --scope platform --yes-shared` | the above plus Data Hubs (Kafka, NiFi, Flink), the CDW Impala warehouse, the CDE virtual cluster and the CAI workspace, in dependency order |
| `--only cai,cde` / `--dry-run` | limit layers / print without acting |

Data Visualization has no suspend command in the CDP CLI and is left running. The shared services
are also used by the sibling demos, so platform scope is for the end of a demo season, not a day.
