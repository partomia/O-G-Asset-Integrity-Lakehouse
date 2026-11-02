# Lakehouse backbone pattern

Proven in **EV-Factory-to-Field-Lakehouse** (EVX): one shared lakehouse, many use cases, demo-ready
at M1 without rewriting for M3.

## Data products

- Domains (`supply`, `cell`, `pack_vehicle`, `field`, …) each own bronze → silver → gold tables,
  a synthetic owner role, a freshness SLA, and a glossary term.
- Versioned **product contracts** under `contracts/products/`; **`config/consumers.json`** lists
  which use case may read/write which columns; CI test diffs contract versions.
- Landing layout: `s3a://…/rsingh_<xx>/landing/<source>/<date>/` with `_manifest.json` per source.

## Core / use-case boundary

| Layer | Owns |
|---|---|
| **Core** | Generators, landing, bronze → gold, shared `features.*`, `semantic.*`, `ref.*` |
| **Use case** (`usecases/ucNN_*`) | Feature SQL, train/gate/score, `gold.ucNN_*`, `app_page.py`, optional `media_page.py` |

Adding uc02 must not change core files (verify with `git diff` on core paths). Shared ML lives in
`mlcore/` (gate, registry, deploy, hashlock, split).

## Config-driven orchestration and UI

- **`usecases/*/config.yaml`** `enabled: true` drives DAG feature/score tasks (discovered at deploy).
- **`app/discover.py`** loads `app_page.py` and optional `media_page.py` per enabled use case.
- **`dataviz/build_dashboard.py`** imports dashboards from JSON specs; primary keys in a free block
  (check all siblings before first import).

## Point-in-time features and splits

- Factory features: no outcome columns; label table separate (`gold.uc01_*_example`).
- **Batch-grouped time split:** latest ~20% of cell batches = TEST; no vehicle leakage across packs.
- **Feature hash lock:** salt `platform` on CDE, `ci` locally; deploy refuses CI salt.
- Tests: PIT SQL names no `fact_dtc` / warranty / telematics; leakage tests on split boundaries.

## Planted causes and recon

- Generator writes **`ref.generator_truth_*`** for causes and faults.
- **`config/expected_recon.json`** lists every allowed MISMATCH/EXPLAINED; recon job fails on surprises.
- A fault keyed on a date may repeat per generator partition — truth table holds every instance.

## Incremental load, late data, WAP

- Gold writes through one **`publish()`** helper: M1 = `overwritePartitions`; M2 = WAP branch inside
  the same signature (spike result in `docs/WAP_SPIKE.md`).
- Late EDR (72 h SLA) reprocesses affected dates only; `ref.reprocess_log` audits.
- Table maintenance and compaction in the DAG maintenance task (M2).

## Freshness

- **`ref.freshness_*`** and semantic SLA views; dashboard “Streaming & Freshness” (M2).
- Streams: one continuous run **or** nightly `available-now` from the DAG — never both on the same
  checkpoint.

## Operations

- **`scripts/platform_ctl.py`** + **`config/platform.json`**: stop/start project scope (CAI apps,
  models, schedules, CDE runs, NiFi group).
- **`ci/sync_code.py`**: `git reset --hard origin/main`, pip once per requirements hash.
- **`ci/project_stop.py`**: scheduled one-off stop; clears its own cron.
- **`docs/DEMO_RUNBOOK.md`** + **`scripts/m1_walkthrough_check.py`**: presenter path and automated SQL/CDV check.

## Model and guardrails (M1 minimum)

- Champion selected by grouped OOF **PR-AUC** (not AUROC alone); logistic can win.
- Isotonic calibration; worklist/abstain thresholds from OOF quantiles — do not retune to force band sizes.
- Guardrails: contracts/quarantine, genealogy completeness, OOD, abstain band, safety floor from
  batch history DTCs/EDR, feature hash, KPI gate; **`action` always NONE** at M1.
- Registry before deploy; **`cdsw-build.sh`** installs `requirements-model.txt` for the model image.

Reference implementation: `EV-Factory-to-Field-Lakehouse` on the `federal` environment.
