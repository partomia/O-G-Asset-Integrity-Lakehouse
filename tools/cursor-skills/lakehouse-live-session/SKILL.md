---
name: lakehouse-live-session
description: >-
  Runs a Cloudera lakehouse demo on the shared federal cluster: project
  start and stop, CDE capacity, streaming, the nightly DAG, NiFi, semantic
  views, and the live proof. Use when starting or stopping a demo, deploying
  CDE jobs, Kafka topics, a NiFi flow, or a DAG, checking virtual-cluster
  headroom, or regenerating live dates.
---

# Live session

Work from the repo that owns the demo. `scripts/platform_ctl.py` is the only start/stop path. Read status before the first change and after the last one.

Shared Data Hubs, the Impala warehouse, the CDE virtual cluster, and the CAI workspace serve every sibling demo. Start or stop one of them only when the user has named it in this conversation. Project scope is the default.

## Capacity

Headroom is the quota minus the CPU and memory requests of runs in `starting` or `running`. The virtual cluster's "in use" figure can stay over the quota with nothing running. Do not size a job up to match it, and do not raise the quota.

One batch job at a time. If the running streams plus that job do not fit, shrink the streams (one executor, one core) and record the change in `config/platform.json` `resources.changes` and `docs/PROJECT_LOG.md`. A size change applies on the next run of that job, so create the definition and then stop and start that job only. Checkpoints stay.

A run whose name is not this project's prefix is not yours to stop.

## Start and stop

`start` brings up this project's model, apps, NiFi processors, and continuous streams, and unpauses its CDE schedules.

- The telemetry producer stays off until the user asks for it. A date that already has live rows is not produced again: ids are deterministic and bronze appends.
- Unpausing a nightly schedule whose window has ended does not fire a run when catchup is off. Say so. Do not move the window unless the user names the new end.
- A manual DAG run skips the stream tasks. Use it to rebuild a date without touching the topics.
- While the nightly window is still open, the DAG is the only thing that runs the streaming jobs. A second continuous run would share their checkpoints.
- NiFi "Hold" processors stay stopped. A group start would drop the failures queued in front of them.
- A model deployment that the API still lists but the serving backend has dropped is deployed again from the same build.
- After a session the user did not ask to keep up, `stop` at project scope. Data, topics, and checkpoints stay.

## Streaming and NiFi

- `--setup` creates a missing topic and does not change the partitions or retention of a topic that exists.
- Each new stream has its own checkpoint directory and its own quarantine table, and its bronze table holds only its own topic.
- The producer takes a topic filter, so a drain of new topics does not resend an existing one.
- Control totals for a new topic sit on the existing control topic. The bronze job that already reads that topic picks them up. Do not replay the data topic to fill the gap.
- Deploy a new NiFi flow stopped, after its topics exist. Re-export it and fail the step if the export contains a password, token, keystore password, or access key. The flow uses the cluster's existing truststore and does not modify it.

## Proof before calling it done

Run the local chain at CI scale before the first live job: land, bronze, extract, silver, master, gold, recon, semantic views. `train` and `score` print SKIPPED at CI scale when scoring is designed to skip.

- On local Spark, a semantic view whose gold table was never written is skipped with a printed reason, and so is a view that reads a skipped view. Impala still fails on a missing table.
- A value from a config file goes into SQL only after a strict pattern check. Quote a literal. Reject a bad value. Do not delete characters from it to make it safe.
- Do not put `scripts/` on `sys.path`. A module there shadows the standard library. Load a script by file path.
- Every MISMATCH, EXPLAINED, and MISSING recon row is a row in `config/expected_recon.json`. A planted fault whose delay sits inside the normal range is moved past that range. It stays out of `built_milestones` until the cluster data for that date is regenerated.
- Compare a stream's control total to its bronze count per topic. An empty quarantine and an unchanged older date are part of the same check.

## Record

Append run ids, counts, and the size change to `docs/PROJECT_LOG.md`. No secret values, no job environments. One commit, hooks on, no `--trailer`. Before pushing, `git log -1 --format=%B` has no `Co-authored-by` or `Generated-with` line.

A force-push makes the CDE git repository refuse to sync. Point the jobs at a temporary repository, recreate the git repository, and point them back. Do that before the next deploy.
