---
name: lakehouse-parallel-phase
description: >-
  Splits one Cloudera lakehouse milestone across parallel agents and merges it
  without overlapping edits. Use when a phase is too large for one agent, when
  the user asks for parallel lanes, worktrees, an integrator, or a contract
  between agents, or when several branches must land on one lakehouse repo.
---

# Parallel phase

One integrator owns the merge. Lanes do not see each other's branches. The contract is the only shared spec, and its first section overrides the rest.

## Before any lane starts

1. Freeze a base commit. Every lane branches from that commit, not from another lane.
2. Write the contract (a file the integrator owns, outside the lanes' commits). Section 0 is the decision list. Later sections are background.
3. Name the merge order before work starts.
4. Split paths so each file has one writer. A file two lanes would both edit is integrator-only.
5. Integrator-only, unless a lane is told otherwise: the DAG, shared job helpers, recon and its expected rows, `config/platform.json`, `config/lakehouse.json`, governance, the generator entrypoint, `run_local.py`, `check_local.py`, `platform_ctl.py`, CI, `PROJECT_LOG.md`, `PLAN.md`, `README.md`, existing tests, and requirements.

## What a lane delivers

- One commit on the base. Hooks on. No `--no-verify`, no `--trailer`, no `Co-authored-by` or `Generated-with`.
- Only the paths it owns. `git show --stat` is the check.
- `pytest -q` green in its own worktree. A skipped test is named. A skipped security or export test is a fail.
- A report: commit id, pytest count, files touched, anything it could not prove.

A lane that needs a file it does not own proposes a patch. The integrator applies it.

## What the integrator does

1. Accept or reject each lane from its report and `git show`. Do not merge a lane that touched another lane's path.
2. Squash a lane that used more than one commit down to one, still on the base.
3. Merge in the declared order. Resolve overlaps by hand and say which side won.
4. Run the whole suite on the merged tree, then the local chain at CI scale (generate, land, bronze, extract, silver, master, gold, recon, semantic views) before any live call.
5. A failure that also fails on the base is a pre-existing defect. Fix it in its own commit, not inside a lane commit.
6. Live work is a separate step. Follow `lakehouse-live-session`. Lanes do not register jobs, deploy flows, or call the cluster.

## Contract rules that kept this shape working

- A new stream has its own topic, checkpoint, and quarantine table. It does not write another stream's table.
- A new bronze source is a contract file the existing ingest loop already discovers. Prove it with one local land and bronze day.
- A module two lanes need is owned by one lane. The other calls it and tolerates `ImportError` until the merge.
- A planted fault that sits inside the normal delay range cannot be detected. Move the plant past that range, and leave it out of `built_milestones` until the cluster data is regenerated.
- New columns that identify a person, a place, or a contact get a governance tag with `propagate: false`.
