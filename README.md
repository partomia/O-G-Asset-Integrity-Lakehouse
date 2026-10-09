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
| Streaming bronze (exactly-once foreachBatch) and 1- and 15-minute windows (watermark, MERGE) | CDE Spark 3.5 Structured Streaming | `cde/jobs/stream_telemetry_bronze.py`, `stream_telemetry_agg.py` |
| Land, bronze, object catalog, quarantine | CDE | `cde/jobs/land_sources.py`, `ingest_bronze.py` |
| Extraction: PDF text and OCR, drawing tags, SEG-Y and LAS headers, video keyframes | CDE (standard library parsers) | `extract/`, `cde/jobs/extract_unstructured.py` |
| Silver, asset master (match rules, golden asset, review queue), gold star schema with SCD2 | CDE + Iceberg | `build_silver.py`, `build_asset_master.py`, `build_gold.py` |
| Reconciliation at every layer: MATCHED / EXPLAINED / LATE / MISMATCH | CDE | `cde/jobs/reconcile.py` |
| Certified KPI views, MIS and dashboard views, KPI consistency check | CDW Impala | `sql/semantic/`, `scripts/run_semantic.py` |
| Dashboards (PKs 13000+) | CDW Data Visualization | `dataviz/build_dashboard.py` |
| Corrosion severity model on drone keyframes: features, train and validate, KPI gate, deploy | CAI jobs `ogx-01` to `ogx-04`, model `ogx-integrity` | `features/`, `train/`, `gate/`, `serve/`, `ci/cai_jobs.py` |
| Integrity Workbench (worklist, Asset 360 with scored keyframes, sensors, data quality) | CAI application | `app/` |
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
python ci/setup_cai.py --app               # CAI jobs ogx-01..04 and the Workbench application
python scripts/governance.py apply
```

The CAI project is created from the public repo URL (no deploy key). Set `OGX_IMPALA_USER` and
`OGX_IMPALA_PASSWORD` in the project environment **before** starting the application, or restart it
afterwards; otherwise it shows a fallback page with no tabs.

## Corrosion model

Keyframes from the drone video are scored by a 49-feature classical model (`features/feature_logic.py`,
version `hc-1.0.0`). `train/train_validate.py` picks between random forest, extra trees and gradient
boosting by 5-fold out-of-fold AUROC and sets the threshold at 0.90 sensitivity; `gate/kpi_gate.py`
blocks promotion on any miss. Current champion (gradient boosting): test AUROC 0.862, sensitivity
0.931, specificity 0.513, Brier 0.124. Bands: P1 (p ≥ 0.80), P2 (p ≥ threshold), P3.

## Status and known issues

- Built and live: phases 0 to 7, streaming, semantic layer, dashboards, governance, Workbench,
  corrosion model chain.
- Model endpoint `ogx-integrity` is deployed but returns 400 on every call; the Workbench scores
  in-process and does not depend on it. Next step: check the replica logs in the CAI UI.
- Not built yet: frame QC and equipment-risk model heads, guardrails, lakehouse scoring and outcome
  tables, silent trial (PLAN phases 8 to 10, partial).

Demo walkthrough: [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md). Build log and every plan change:
[docs/PROJECT_LOG.md](docs/PROJECT_LOG.md). Plan: [PLAN.md](PLAN.md).
