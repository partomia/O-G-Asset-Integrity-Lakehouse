# MLOps on Cloudera AI: training, retraining, registry

## The chain (CAI jobs, `ci/cai_jobs.py`)

```
ogx-00-sync-code ─► ogx-01-build-features ─► ogx-02-train-validate ─► ogx-03-kpi-gate ─► ogx-04-deploy-champion
 git reset to the     features + engineer      3 candidates, 5-fold      absolute KPIs +       CAI model ogx-integrity,
 pushed commit        frame labels             OOF AUROC, MLflow run     non-regression        AI Registry version
```

A dependent job starts only when its parent succeeds, so the gate's exit code is a hard stop:
a rejected candidate never reaches the endpoint and the champion keeps serving.

## What starts it

| Trigger | How | Reason recorded |
|---|---|---|
| Code change | `git push` → GitHub Actions `.github/workflows/cai-mlops.yml` → `ci/trigger_cai_pipeline.py` → CAI API v2 starts `ogx-00` with `EXPECTED_GIT_SHA` and follows the chain; the check goes red when the gate stops it | `github push <sha>` |
| Drift | `ogx-05-nightly-drift` (02:00) → `outputs/monitoring/drift_report.json` ALERT → `ogx-08-retrain-trigger` (02:30) starts `ogx-01` | `drift (psi_score, ...)` |
| Engineer feedback | ≥ 20 frames labelled in Asset 360 since the champion was trained (`outputs/feedback/frame_labels.csv`) → `ogx-08` | `feedback (N new engineer labels)` |
| Staleness | champion older than 30 days → `ogx-08` | `age (N days)` |
| A person | `python ci/setup_cai.py --run ogx-08-retrain-trigger --env OGX_RETRAIN_FORCE=1` | `forced` |

The reason travels as `OGX_TRIGGER` into the feature table's `meta.json`, the MLflow run's
parameters and every model event.

## Feedback loop

An engineer confirms or corrects a keyframe's severity in **Asset 360** ("Guardrails and
review" → Save label). `features/build_feature_table.py` adds the latest label per frame to the
TRAIN split, except frames cut from a TEST image (they would leak into the held-out evaluation).

## Versions and lineage

- **MLflow** (CAI experiment `ogx-integrity`): one run per training with parameters
  (estimator, feature version and hash, threshold, git sha, training and feedback rows,
  trigger), valid and TEST metrics, the scikit-learn model and `model_meta.json`; the gate adds
  `kpi_gate_passed`.
- **Cloudera AI Registry**: `serve/registry.py` registers the run's model as a new version of
  `ogx-corrosion` when `ogx-04` deploys it (stage champion). This workbench stores no version
  tags, so the stage and version number are also in `ref.model_event`.
- **`<prefix>_ref.model_event`** (Iceberg, written by every job): `TRAINED`, `GATE_PASSED` /
  `GATE_FAILED`, `DEPLOYED`, `REGISTERED`, `ROLLED_BACK`, `DRIFT_ALERT`, `RETRAIN_TRIGGERED` /
  `RETRAIN_SKIPPED`, with model version, KPIs, gate checks, MLflow run id and reason.
- **`ref.guardrail_event`** and **`ref.model_drift`**: see [GUARDRAILS.md](GUARDRAILS.md).

The Workbench's **Models & guardrails** tab reads all three tables and the champion's
`model_meta.json`.

## Commands

```bash
set -a; source .env; set +a
python ci/setup_cai.py                                  # creates ogx-05 / ogx-08 (idempotent)
python ci/setup_cai.py --run ogx-05-nightly-drift       # drift + guardrail events now
python ci/setup_cai.py --run ogx-08-retrain-trigger --env OGX_RETRAIN_FORCE=1   # retrain now
python ci/retrain_trigger.py --dry-run                  # decide, start nothing (in a CAI session)
```

GitHub secrets for `cai-mlops.yml`: `CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID`
(until they are set, the workflow prints a notice and stays green).
