# O&G Asset Integrity Lakehouse

An oil and gas asset-integrity lakehouse on Cloudera (CDP Public Cloud): ERP registers and work
orders, inspection PDFs (vector and scanned), P&ID drawings, SEG-Y seismic, LAS well logs, drone
video and streaming SCADA telemetry, resolved to **one golden asset** and ranked by integrity risk
for the engineer who has to decide what to inspect first.

All data is synthetic (seeded generators in `generators/`), apart from the drone keyframes, which
come from a CC BY 4.0 corrosion image set (`assets/frames/ATTRIBUTION.md`).

## What runs where

| Layer | Cloudera service | Code |
|---|---|---|
| Kafka topics `ogx.sensor.telemetry`, `ogx.scada.alarm`, `ogx.control`, Schema Registry | Streams Messaging Data Hub `federal-kafka` | `stream/producer/`, `cde/jobs/stream_produce.py` |
| Alarm routing: validate on the Schema Registry contract, priority alarms to `ogx.alarm.priority`, raw landing to S3, versioned in NiFi Registry | NiFi Data Hub `federal-nifi` (NiFi 2.6, NiFi Registry bucket `ogx-flows`) | `nifi/deploy_flow.py`, [docs/NIFI.md](docs/NIFI.md) |
| Streaming bronze (exactly-once foreachBatch) and 1- and 15-minute windows (watermark, MERGE) | CDE Spark 3.5 Structured Streaming | `cde/jobs/stream_telemetry_bronze.py`, `stream_telemetry_agg.py` |
| Land, bronze, object catalog, quarantine | CDE | `cde/jobs/land_sources.py`, `ingest_bronze.py` |
| Extraction: PDF text and OCR, drawing tags, SEG-Y and LAS headers, video keyframes | CDE (standard library parsers) | `extract/`, `cde/jobs/extract_unstructured.py` |
| Silver, asset master (match rules, golden asset, review queue), gold star schema with SCD2 | CDE + Iceberg | `build_silver.py`, `build_asset_master.py`, `build_gold.py` |
| Reconciliation at every layer: MATCHED / EXPLAINED / LATE / MISMATCH | CDE | `cde/jobs/reconcile.py` |
| Certified KPI views, MIS and dashboard views, KPI consistency check | CDW Impala | `sql/semantic/`, `scripts/run_semantic.py` |
| Dashboards (PKs 13000+) | CDW Data Visualization | `dataviz/build_dashboard.py` |
| Corrosion severity model on drone keyframes: features, train and validate, KPI gate, deploy | CAI jobs `ogx-01` to `ogx-04`, model `ogx-integrity` | `features/`, `train/`, `gate/`, `serve/`, `ci/cai_jobs.py` |
| Model versions: MLflow runs, AI Registry `ogx-corrosion`, lifecycle in `ref.model_event` | CAI experiments + AI Registry, CDW | `serve/registry.py`, `lakehouse/` |
| Guardrails: frame quality, out of distribution, abstain band, safety floor, no automated action | CAI model + application | `guardrails/`, `config/guardrails.yaml`, [docs/GUARDRAILS.md](docs/GUARDRAILS.md) |
| MLOps: nightly drift, retrain trigger (drift, engineer labels, age), GitHub push to CAI chain | CAI jobs `ogx-05`, `ogx-08`, GitHub Actions `cai-mlops.yml` | `monitor/drift.py`, `ci/retrain_trigger.py`, [docs/MLOPS.md](docs/MLOPS.md) |
| Integrity Workbench (worklist, Asset 360 with scored keyframes, sensors, data quality, models & guardrails) | CAI application | `app/` |
| Classifications, glossary, tag masking | SDX: Atlas, Ranger | `scripts/governance.py`, `config/governance.json` |

Databases: `rsingh_ogx_{bronze,silver,asset,gold,semantic,ref}`. Objects: `s3a://.../rsingh_ogx/`.

## Certified KPIs (`config/kpi.json`)

- **IRE, integrity risk exposure**: sum of p(integrity event in 30 days) x criticality weight;
  assets with a stuck sensor are abstained, not guessed.
- **UNC, unstructured coverage**: objects extracted and linked to an asset over objects received.
- **TTR, severe-defect time to review**: hours from capture to review, risk-ranked worklist vs
  calendar order.

Every MIS view, dashboard tile and the app read these views; `run_semantic.py check` proves each
consumer reproduces the certified figure (results in `ref.recon_results`, layer `semantic`).

## Planted faults the pipeline must catch

| Fault | Where it shows |
|---|---|
| Corrupt inspection PDF on 10-05 | quarantined; recon MISMATCH on landed objects |
| Seismic file resent on 10-05 | DUPLICATE; recon EXPLAINED |
| Work-order trailer total wrong on 10-06 | recon MISMATCH on control total |
| Stuck vibration sensor VI-112B from 10-05 12:00 | stuck windows; asset abstained from IRE |
| PI-105A switches psi to bar on 10-06 10:00 | normalised to psi; bar readings counted |
| ~0.1 % of readings very late | LATE, past the watermark |
| Pipeline segment named in one source, GPS 38 m from another | asset master conflict, auto-resolved, review queue |

## Run it

Local (Spark 4 + Iceberg, no cloud):

```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-ci.txt
pytest -q
python scripts/run_local.py all --dates all --landing /tmp/ogxw/landing --warehouse /tmp/ogxw/warehouse
```

Cloudera (credentials in a git-ignored `.env`, see `.env.example`):

```bash
set -a; source .env; set +a
./cde/scripts/deploy_jobs.sh                 # CDE repository + batch jobs
./cde/scripts/deploy_streaming.sh create     # streaming jobs and the producer job
./cde/scripts/deploy_streaming.sh start
cde job run --name rsingh-ogx-stages --arg=--stages --arg=silver,asset,gold,recon --arg=--dates --arg=2026-10-08 ...
python scripts/run_semantic.py --engine impala --steps views,check,adhoc
python dataviz/build_dashboard.py --import --connection federal-impala-1
python ci/setup_cai.py --app               # CAI jobs ogx-00..08 and the Workbench application
python scripts/governance.py apply
python nifi/deploy_flow.py deploy           # NiFi alarm router, version 1 in NiFi Registry
```

The CAI project is created from the public repo URL (no deploy key). Set `OGX_IMPALA_USER` and
`OGX_IMPALA_PASSWORD` in the project environment **before** starting the application, or restart it
afterwards; otherwise it shows a fallback page with no tabs.

## Corrosion model

Keyframes from the drone video are scored by a 49-feature classical model (`features/feature_logic.py`,
version `hc-1.0.0`). `train/train_validate.py` picks between random forest, extra trees and gradient
boosting by 5-fold out-of-fold AUROC and sets the threshold at 0.90 sensitivity; `gate/kpi_gate.py`
blocks promotion on any miss and on a TEST AUROC loss of more than 0.02 against the champion.
Current champion (gradient boosting): test AUROC 0.862, sensitivity 0.931, specificity 0.513,
Brier 0.124. Bands: P1 (p ≥ 0.80), P2 (p ≥ threshold), P3, plus NA (failed input guardrail) and
UNCERTAIN (within 0.05 of the threshold); criticality A never below P2.

Every training is an MLflow run; every deployed champion is a new version of `ogx-corrosion` in
the Cloudera AI Registry; every step (trained, gate, deployed, registered, drift alert, retrain
triggered or skipped) is a row in `ref.model_event`. Retraining starts on a push (GitHub Actions),
on a drift alert, on 20 new engineer labels from Asset 360 or when the champion is 30 days old.

## Status and known issues

- Built and live: phases 0 to 7, streaming, NiFi alarm routing, semantic layer, dashboards,
  governance, Workbench, corrosion model chain with guardrails, registry versions and the
  retraining loop.
- Not built yet: frame QC and equipment-risk model heads, silent trial and named-approver
  promotion (`ogx-07`), lakehouse scoring job (`ogx-06`), the LLM report-summary guardrail.
- Model endpoint `ogx-integrity` serves AI Registry `ogx-corrosion` v2 (retrained 10-15, two runs
  in the CAI experiment), 1 vCPU / 2 GB, about 0.4 s per keyframe. Model builds that stall are
  replaced after 10 minutes; registration is idempotent per MLflow run.
- CDE Airflow DAG `rsingh-ogx-orchestration` validated end to end (run 627, all stages succeeded).
- Final demo window: the DAG runs nightly at 02:00 IST for business dates 2026-10-16 .. 10-19
  (each night also replays that day's sensor stream), then its `end_date` stops it. CAI job
  `stop-ogx-project` fires once on 2026-10-20 15:30 IST and stops the app, the model endpoint, the
  nightly CAI schedules and the NiFi flow. Bring it back with `python scripts/platform.py start`.

## Enhancements (2026-10-15)

| Area | What changed | Where |
|---|---|---|
| Model endpoint | the model function is wrapped by `cml.models_v1.cml_model`, which PBJ runtimes need (without it every call answered "model busy"); a load failure is returned as an error instead of crashing the replica | `serve/predict.py` |
| AI Registry | `ogx-04` registers the champion **before** the endpoint deploy, so a slow or failed deploy keeps the version (`ogx-corrosion` v1); deploy wait configurable (`cai.deploy_timeout_s`) | `serve/deploy_champion.py`, `config/pipeline.yaml` |
| Workbench | 2 vCPU / 8 GB; the launcher restarts Streamlit if it exits; one Impala connection per session thread with reconnect; keyframe scores cached | `app/launch_app.py`, `app/app.py`, `ci/setup_cai.py` |
| Retraining | the skip event says which rule held (drift cooldown, labels, age) | `ci/retrain_trigger.py` |
| Orchestration | CDE Airflow DAG `ogx_asset_integrity` (land → bronze → extract → silver → asset → gold → recon) as job `rsingh-ogx-orchestration` | `cde/dags/ogx_dag.py`, `cde/scripts/deploy_dag.sh` |
| Operations | stop, start and status of the whole demo across CAI, NiFi, CDE, CDW, Flink and Kafka | `scripts/platform.py`, `config/platform.json` |
| Reuse | Cursor skill `cloudera-usecase-builder` and the playbook for the next use case | `tools/cursor-skills/`, [docs/USE_CASE_PLAYBOOK.md](docs/USE_CASE_PLAYBOOK.md) |

## Demo day

```bash
set -a; source .env; set +a
```

Before (30 to 60 minutes ahead):

1. `python scripts/platform.py status --scope platform`: every Data Hub AVAILABLE, the Impala
   warehouse Running, the CDE virtual cluster AppInstalled, the CAI workspace finished.
2. `python scripts/platform.py start`: Workbench, model deployment, job schedules, the two CDE
   streaming jobs (they resume from their checkpoints) and the NiFi alarm router.
3. Live sensor feed for the session (3 hours):
   `cde job run --name rsingh-ogx-stream-produce --arg=--mode --arg=live --arg=--duration --arg=10800`
4. Optional fresh batch: `cde job run --name rsingh-ogx-orchestration --config-json '{"business_date": "2026-10-08"}'`,
   then `python scripts/run_semantic.py --engine impala --steps views,check --dates 2026-10-08`.
5. Open the Workbench and the four OGX dashboards once, so the first screen in front of the
   audience is warm.

During: follow [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md) (about 35 minutes).

After:

1. `python scripts/platform.py stop`: this demo's workloads only (shared services keep running).
2. End of a demo season, with the owners of the sibling demos agreeing:
   `python scripts/platform.py stop --scope platform --yes-shared`; bring it back with
   `start --scope platform --yes-shared` (Kafka first, CAI last, about 30 to 45 minutes).

Demo walkthrough: [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md). Build log and every plan change:
[docs/PROJECT_LOG.md](docs/PROJECT_LOG.md). Plan: [PLAN.md](PLAN.md).
