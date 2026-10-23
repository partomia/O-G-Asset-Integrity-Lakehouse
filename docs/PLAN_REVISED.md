# O&G Asset Integrity Lakehouse: the build prompt, revised to what was built

`PLAN.md` (2026-10-08) is the prompt the project was built from. This file rewrites it as it would
read if written today, knowing what the build ran into. Each departure is marked in place:

- **[ADDED]** built, but not asked for in the plan
- **[CHANGED]** asked for, built differently (the reason follows)
- **[NOT BUILT]** asked for, still open
- unmarked text was built as the plan said

Evidence for every change is in `docs/PROJECT_LOG.md` (dated entries); state as of 2026-10-23.

## Change map

| Area | Plan said | Built | Why |
|---|---|---|---|
| Video format | MP4 via OpenCV | Motion-JPEG AVI | standard library only, no python-env resource on CDE |
| Generators and parsers | reportlab, OpenCV, segyio, lasio | minimal standard-library PDF, PNG, AVI, SEG-Y, LAS writers and readers | same |
| OCR | general OCR fallback | template matching on the synthetic scanner font | reads every field exactly; Tesseract would sit in the same slot |
| Severity label | corrosion area >= 15 % = severe | >= 2 % of the frame or >= 3 boxes | 15 % gave 4 severe images in 1,249 |
| Corrosion features | frozen image embeddings (ViT) | 49 colour and texture features (`hc-1.0.0`), tree ensembles | no GPU; fits 1 vCPU; slim model image |
| Threshold | on VAL, severe recall >= 0.95 | from 5-fold out-of-fold scores, sensitivity 0.90 | VAL alone set 0.002 and failed specificity |
| Model heads | corrosion, frame_qc, equipment_risk | corrosion only | the other two are open |
| frame_qc head | a model | rule-based frame checks plus an OOD distance | stands in until the head exists |
| Endpoint | one registered model per head | one CAI model `ogx-integrity`, AI Registry `ogx-corrosion` (v1, v2) | one head |
| App scoring | endpoint for on-demand re-score | in-process with the champion files; endpoint verified separately | endpoint was unreliable for days |
| Producer | CAI job | CAI job and CDE job `rsingh-ogx-stream-produce` | brokers' port 9093 is reachable from CDE, not from the laptop |
| Alarm routing | none | NiFi flow, topic `ogx.alarm.priority`, NiFi Registry | added |
| Stuck-sensor rule | zero variance across N windows (90 readings) | minimum 24 readings per 15 minutes | producer samples every 30 s, rule never fired |
| Breach limit | one per measure | per-tag overrides (wellhead pressure 2,500 psi) | every well window breached |
| IRE | equipment_risk probability | gold rule score / 100 (`p_source = rule_score`) | no equipment_risk head yet |
| Orchestration | DAG per business date that calls ogx-06 | DAG without ogx-06; manual runs by date; nightly window that also replays the day's stream | ogx-06 not built |
| Streaming jobs | long-running | long-running for live demos; during a nightly window, each DAG run starts them with available-now | they share one checkpoint |
| Feedback | `gold.review_feedback` | CSV files under `outputs/feedback/` counted by the retrain trigger | built with the app, before a gold table |
| Retraining | not specified beyond the chain | `ogx-08` trigger: drift (weekly), 20 labels, 30 days age, git push | added |
| Data Visualization keys | 9500+ | 13000+ | GDL's visuals reach 9508 |
| Sizing | not specified | CAI jobs 1 vCPU / 4 GB, app 2 vCPU / 8 GB, model 1 vCPU / 2 GB | 2 / 8 jobs sat in scheduling |
| Code delivery to CAI | `ogx-00` git pull | `ogx-00`, and `ci/push_files.py` (files API) when git could not pull | repo was private from 10-15 for a while |
| Operations | none | `scripts/platform_ctl.py` start, stop, status; Cursor rule; self-stop job | added |

## Overview

Build a third sibling repo, O-G-Asset-Integrity-Lakehouse, that proves one claim: a lakehouse can
store, index and use unstructured oil and gas data (PDFs, seismic, drawings, inspection video)
beside structured tables and live sensor streams, on one governed Cloudera platform. It copies the
General-DataLakehouse spine (synthetic sources, medallion on Iceberg, reconciliation, certified
KPIs, governance as code) and the Chest-X-ray-Triage ML spine (one feature definition, KPI gate,
AI Registry, CI/CD to CAI, Streamlit app). Silent trial: **[NOT BUILT]**.

The story, the decision supported (risk-based instead of calendar-order inspection, workflow
prioritisation only, no automated work orders) and the scale are unchanged.

## Requirements map

| # | Requirement | Where it is met |
|---|---|---|
| 1 | Unstructured data beside relational tables | raw objects by reference in S3, `bronze.doc_object`, extractors to silver. `docs/UNSTRUCTURED.md` **[NOT BUILT]** |
| 2 | PDFs, seismic, drawings, video (+ LAS) | `generators/`, `extract/` **[CHANGED]**: standard library only, video is Motion-JPEG AVI, OCR is template matching |
| 3 | Kafka stream, generated and produced | `stream/producer/`, plus **[ADDED]** CDE job `rsingh-ogx-stream-produce` (`cde/jobs/stream_produce.py`, backfill and live modes) |
| 4 | CDE Spark streaming from Kafka | `stream_telemetry_bronze.py`, `stream_telemetry_agg.py`, **[ADDED]** exactly-once commits (each batch tagged `ogx.stream-batch = <query>:<batch>`, replays skipped) |
| 5 | CDE batch medallion | `land_sources.py` .. `build_gold.py`, `reconcile.py`, DAG `cde/dags/ogx_dag.py`. **[ADDED]** `rsingh-ogx-stages` (`run_stages.py`: several stages and dates in one Spark session). `build_outcomes.py` / `rsingh-ogx-outcomes` **[NOT BUILT]** |
| 6 | CDW | `sql/semantic/` KPI, MIS and dashboard views; ad-hoc queries in `run_semantic.py --steps adhoc`. `sql/adhoc.sql`, `unstructured_search.sql`, `time_travel.sql` **[NOT BUILT]** as files |
| 7 | CDV | `dataviz/build_dashboard.py`, four dashboards **[CHANGED]**: PKs 13000+ |
| 8 | CAI Workbench | one project; jobs `ogx-00` .. `ogx-05`, **[ADDED]** `ogx-08-retrain-trigger`, `stop-ogx-project`; `ogx-06`, `ogx-07` **[NOT BUILT]** |
| 9 | CAI feature engineering | `features/feature_logic.py` (only definition), versioned table + feature hash **[CHANGED]**: classical colour and texture features, not embeddings |
| 10 | AI Registry versioning | `serve/registry.py`: a version per deployed champion **[CHANGED]**: registered before the endpoint deploy, idempotent per MLflow run. Silent trial and promotion versions **[NOT BUILT]** |
| 11 | Guardrails | `guardrails/`, `config/guardrails.yaml`, `docs/GUARDRAILS.md`. LLM guardrail **[NOT BUILT]** (summaries off, as allowed) |
| 12 | Application | `app/` Streamlit Integrity Workbench |
| 13 | CI/CD | `ci.yml`, `cai-mlops.yml` (needs GitHub secrets `CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID`) |
| 14 | Governance | `scripts/governance.py`: classifications, glossary, Ranger tag masking for federal01 / federal07 |
| 15 | **[ADDED]** Alarm routing | NiFi on `federal-nifi`: validate against Schema Registry, priority alarms to `ogx.alarm.priority`, raw landing to S3, versioned in NiFi Registry (`nifi/`, `docs/NIFI.md`) |
| 16 | **[ADDED]** MLOps loop | MLflow run per training, `ref.model_event`, `ref.guardrail_event`, `ref.model_drift`; `ogx-05` drift and `ogx-08` retrain trigger (`docs/MLOPS.md`) |
| 17 | **[ADDED]** Operations | `scripts/platform_ctl.py` + `config/platform.json`: status, stop, start across CAI, NiFi, CDE, CDW, Flink, Kafka, project or platform scope; `.cursor/rules/project-start-stop.mdc`; `ci/project_stop.py` |
| 18 | **[ADDED]** Reuse | Cursor skill `cloudera-usecase-builder` (`tools/cursor-skills/`, `reference.md` of platform lessons) and `docs/USE_CASE_PLAYBOOK.md` |

## Platform mapping

As in the plan, with these differences:

- **[ADDED]** NiFi Data Hub `federal-nifi` (NiFi 2.6) for alarm routing; topic
  `ogx.alarm.priority` is kept out of the bronze stream's subscription.
- **[CHANGED]** Kafka admin (topics, the network path) runs from CDE (Java AdminClient over
  SASL_SSL PLAIN) and the SMM REST API, because port 9093 is closed from the laptop.
- **[CHANGED]** Orchestration: the DAG does not call CAI scoring (`ogx-06` not built). Scheduled
  runs (nightly at 20:30 UTC, bounded by `start_date` / `end_date`) add a stream branch before gold:
  producer backfill of the day, then both streaming jobs with an available-now trigger.
  Manual runs take `params.business_date` and skip the stream.
- **[CHANGED]** Model serving: one CAI model `ogx-integrity` on a PBJ runtime; the function must
  be wrapped by `cml.models_v1.cml_model`.

## Names and conventions

Unchanged, except:

- **[CHANGED]** Data Visualization PKs 13000+ (not 9500+).
- **[ADDED]** CDE jobs `rsingh-ogx-stages`, `rsingh-ogx-stream-produce`; files resource
  `rsingh-ogx-dag` was used while the repo was private.
- **[ADDED]** CAI jobs `ogx-08-retrain-trigger`, `stop-ogx-project`; model `ogx-integrity`.
- **[ADDED]** Topic `ogx.alarm.priority`; NiFi process group `OGX - SCADA Alarm Router`; NiFi
  Registry bucket `ogx-flows`.
- **[NOT BUILT]** Registered models `ogx-frame-qc`, `ogx-equipment-risk`; CDE job `rsingh-ogx-outcomes`.
- **[CHANGED]** Repository: public, private from 10-15 (CDE repository sync and CAI git pull
  stopped working), public again by 10-16.

## Synthetic sources and the Kafka producer

Eight sources from one seeded asset universe, as planned, with **[CHANGED]** formats and tools:

| Source | Format | Generator |
|---|---|---|
| Asset register | mysqldump-style SQL, then CDC JSON | `gen_erp.py` |
| Work orders | pipe-delimited CSV with trailer | `gen_erp.py` |
| Inspection reports | PDF with text layer, scanned image-only PDF | standard-library PDF writer (not reportlab) |
| Drone video | **Motion-JPEG AVI** (not MP4) with sidecar JSON; keyframes are the frame-library JPEGs byte for byte | standard-library AVI writer (not OpenCV) |
| P&ID drawings | PDF and PNG | standard library |
| Seismic | SEG-Y | standard library (not segyio) |
| Well logs | LAS 2.0 | standard library (not lasio) |
| Telemetry and alarms | JSON on Kafka, Schema Registry schemas | `sensor_producer.py` |

- **[CHANGED]** Sample rate: one reading per tag every 30 s (not 5 s); 2.88M readings over five days.
- Planted faults as planned. Concretely: corrupt PDF 10-05, seismic resend 10-05, work-order trailer
  off by one 10-06, stuck VI-112B from 10-05 12:00, PI-105A psi to bar 10-06 10:00, about 0.1 %
  very late readings, a pipeline segment 38 m apart in two sources.
- **[CHANGED]** Video with the wrong asset tag in the file name is caught as an asset-master
  conflict and review-queue item.

## Unstructured data handling pattern

Store by reference, extract to tables, link to the asset: as planned. Extractors:

- PDF text per page, **[CHANGED]** OCR fallback by template matching against the synthetic
  scanner's 5x7 font (`extract/ocr.py`).
- Drawing tags, SEG-Y headers, LAS curves: **[CHANGED]** standard-library parsers.
- Video keyframes: **[CHANGED]** read from the AVI by a standard-library reader; blur and exposure
  scores; CAI scoring finds each frame by sha256.
- Vector search over `doc_chunk`: optional in the plan, **[NOT BUILT]**.

## Streaming with CDE Spark

As planned (bronze with contracts and quarantine, aggregation with 10-minute watermark, 1- and
15-minute windows, MERGE through foreachBatch, stream reconciliation on `ogx.control`, stop and
restart drill), plus:

- **[CHANGED]** Trigger written `"60 seconds"` (Spark 3.5 rejects `60s`).
- **[CHANGED]** Stuck-sensor minimum 24 readings per 15 minutes; gold also derives stuck windows from
  window statistics, so the running stream needs no restart for a rule change.
- **[CHANGED]** `threshold_overrides` in `config/streaming.json` (wellhead pressure PI-W* 2,500 psi).
- **[ADDED]** Exactly-once batch marker on each Iceberg commit.
- **[CHANGED]** Operations: continuous runs for a live demo; during a nightly window the DAG runs the
  same jobs with available-now, so `platform_ctl.py start` leaves the continuous runs off.
- Open item resolved: CDE runs long-running streaming jobs; no job timeout.

## Layers, asset master and gold model

Bronze, silver, asset master (candidates, rules, `asset_xref`, `golden_asset`, review queue) and the
gold star schema with SCD2 `dim_asset`, as planned. Differences:

- **[CHANGED]** `fact_inspection_score` and `model_score` are not filled by a lakehouse job; the
  app scores keyframes in process, `ogx-05` scores the lakehouse keyframes for drift.
- **[NOT BUILT]** `fact_review_outcome` and `daily_integrity_summary` as gold tables; TTR is computed
  in the semantic view from extracted findings (severe = wall loss >= 20 %), risk order vs calendar
  order on the same inspections.
- **[NOT BUILT]** The thermography worked extension.

## ML on CAI

**Built: the corrosion head only.**

- **[CHANGED]** Labels: severe = corrosion boxes >= 2 % of the frame or >= 3 boxes; surface = any
  other corrosion; none = no box (dataset Corrosion Bi3Q3, CC BY 4.0, pinned revision).
- **[CHANGED]** Features: 49 colour and texture features with numpy and Pillow (`hc-1.0.0`), not frozen
  image embeddings; random forest, extra trees and gradient boosting chosen by 5-fold out-of-fold AUROC.
- **[CHANGED]** Threshold from the out-of-fold scores at sensitivity 0.90 (the gate minimum is 0.80);
  gate blocks any KPI miss and a TEST AUROC loss above 0.02 against the champion.
  Champion: gradient boosting, TEST AUROC 0.862, sensitivity 0.931, specificity 0.513.
- **[CHANGED]** `ogx-04` registers the AI Registry version first, then deploys; a model build that has
  not finished in 600 s is replaced (up to 3 attempts); the model build context excludes the
  workbench's `.local` / `.cache` home state.
- **[ADDED]** MLflow run per training; `ref.model_event` for TRAINED, GATE_PASSED, REGISTERED,
  DEPLOYED, ROLLED_BACK, DRIFT_ALERT, RETRAIN_TRIGGERED / SKIPPED.
- **[ADDED]** `ogx-08-retrain-trigger` (02:30): retrains on a drift alert (at most weekly: the demo
  frames differ from training by design), 20 new engineer labels, or a 30-day-old champion.
- `ogx-05-nightly-drift` (02:00): PSI on the features of the lakehouse keyframes, `TOO_FEW` below 30.
- **[NOT BUILT]** `frame_qc` and `equipment_risk` heads, `ogx-06-score-inspections`
  (`lakehouse/score_inspections.py`), silent trial, `ogx-07-promote-champion` with a named approver,
  `serve/explain.py` (occlusion heatmap), `serve/promote_champion.py`.

## Guardrails for ML

| Layer | Guardrail | Status |
|---|---|---|
| Input | Object and message contracts to quarantine | built |
| Input | Frame quality | **[CHANGED]** rule checks (size, brightness, contrast, sharpness) instead of the frame_qc head; tuned on 105 clean + 105 degraded frames |
| Input | Out-of-distribution | built: distance limit = training q99.5, stored with the model |
| Input | Sensor sanity (stuck, unit, too few readings) | built; the asset is abstained from IRE |
| Input | Asset not resolved | built: review queue |
| Output | Abstain band (within 0.05 of the threshold) | built |
| Output | No automated action | built |
| Output | Criticality-A floor | **[CHANGED]** criticality A never below P2 (the plan said: never below its calendar due date) |
| Process | KPI gate and non-regression | built |
| Process | Feature hash lock | built |
| Process | Silent trial and named approver | **[NOT BUILT]** |
| Process | Drift threshold | built |
| LLM | Grounded report summaries | **[NOT BUILT]** (optional) |

All events go to `ref.guardrail_event`.

## Application: Integrity Workbench

**[CHANGED]** Five tabs instead of four pages:

- Worklist: risk bands beside calendar order; **[ADDED]** engineer review outcome capture.
- Asset 360: golden record, findings, scored keyframes with the guardrail trace, **[ADDED]** engineer
  labels for retraining. P&ID highlight and the occlusion heatmap **[NOT BUILT]**.
- **[ADDED]** Sensors, Data quality, Models & guardrails tabs.
- Document search **[NOT BUILT]** as a page.
- **[CHANGED]** Feedback goes to `outputs/feedback/*.csv` (not `gold.review_feedback`).
- **[ADDED]** Hardening: the launcher restarts Streamlit if it exits, one Impala connection per
  session thread with reconnect, keyframe scores cached; 2 vCPU / 8 GB.

## KPIs, CDW and CDV

- **[CHANGED]** IRE uses the gold rule score / 100 until equipment_risk exists (`p_source`); stuck
  assets abstained. UNC and TTR as planned (TTR on extracted wall loss).
- **[ADDED]** MIS views and a KPI consistency check (`run_semantic.py check`, results in
  `ref.recon_results`, 9 of 9 MATCHED per date).
- Impala reserved words: `format` and `method` renamed or escaped.
- Four dashboards, 9 datasets, 36 visuals; `--verify` compares tiles with Impala.

## Operations [ADDED]

Not in the plan; built because the shared environment had to be stopped and restarted cleanly.

- `scripts/platform_ctl.py status | stop | start`, `--only`, `--dry-run`; `--scope platform
  --yes-shared` for the shared Data Hubs, CDW warehouse, CDE VC and CAI workspace.
- CAI schedules stop by clearing the cron (API v2 ignores `paused`); start restores them from
  `config/platform.json`. A model deployment that cannot be restarted is redeployed from the same build.
- CDE: schedules paused / unpaused, Spark children killed before orchestration runs.
- `ci/project_stop.py` (CAI job `stop-ogx-project`): a dated one-off stop.
- Cursor rule `project-start-stop.mdc`: start/stop requests always go through the script.

## Repository layout

As planned, minus the **[NOT BUILT]** files above, plus **[ADDED]** `nifi/`, `scripts/platform_ctl.py`,
`config/platform.json`, `ci/push_files.py`, `ci/retrain_trigger.py`, `ci/project_stop.py`,
`cde/jobs/stream_produce.py`, `cde/jobs/run_stages.py`, `assets/frames/` (210-frame library),
`requirements-model.txt`, `tools/cursor-skills/`. **[NOT BUILT]** docs: `DATA_MODEL.md`,
`UNSTRUCTURED.md`, `STREAMING.md`, `GOVERNANCE.md`, `DATAVIZ.md`, `FAILED_BATCH_DEMO.md`,
`PRESENTER_RUNBOOK.md`, and the `capabilities/` folder is empty.

## Phases

| Phase | Status |
|---|---|
| 0 to 7 | done as planned, with the changes above |
| 8 CAI ML chain | partly: corrosion head end to end, registry, drift, retrain trigger, `cai-mlops.yml`; frame_qc and equipment_risk open |
| 9 Guardrails | done, frame checks in place of frame_qc; no LLM guardrail |
| 10 Lakehouse scoring and outcomes | **not built** (`ogx-06`, outcomes job, silent trial, `ogx-07`) |
| 11 CDW, CDV, governance | done |
| 12 Application and docs | app done and extended; several docs, capabilities and thermography open |
| **[ADDED]** NiFi alarm routing | done (2026-10-11) |
| **[ADDED]** Orchestration and nightly window | done (DAG validated run 627; window 10-23 .. 10-26) |
| **[ADDED]** Operations and reuse | done (`platform_ctl.py`, Cursor rule and skill) |

## How we work

Unchanged (copy then adapt, credentials from the siblings' `.env` re-prefixed `OGX_*`, one approval
list, one commit per phase, never touch the siblings), with two exceptions recorded in the log:

- 2026-10-15: on your instruction, other demos' CDE runs, schedules and CAI model deployments were
  stopped to free shared capacity.
- 2026-10-11: part of a token was printed to the terminal from a run's environment; not written
  anywhere, rotation requested. Since then run environments are never printed.

## Decisions and open items

- Binaries by reference, Kafka on Streams Messaging, two CDE streaming jobs (Flink as the noted
  alternative), Data Visualization on the existing instance: as decided.
- **[CHANGED]** Producer on CAI and CDE.
- **[CHANGED]** Corrosion head on classical features (no GPU profile).
- Resolved: repo public; Kafka on `federal-kafka`; NiFi on `federal-nifi`; CDE streaming
  supported, no job timeout; dataset Corrosion Bi3Q3 CC BY 4.0; no GPU; Ranger users federal01 / federal07.
- Still open: confirm the three certified KPIs (or swap one for an inspection-backlog KPI).
