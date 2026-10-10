# ML guardrails

Every score the corrosion model produces passes the same guardrails, in the endpoint
(`serve/predict.py`, CAI model `ogx-integrity`), in the Workbench (Asset 360) and in the nightly
batch (`monitor/drift.py`, job `ogx-05-nightly-drift`). Limits live in `config/guardrails.yaml`;
each guardrail has a test in `tests/test_guardrails.py` that makes it fire.

| Layer | Guardrail | Code | What happens when it fires |
|---|---|---|---|
| Input | Object and message contracts | `contracts/`, `ingest_bronze.py`; NiFi `ValidateRecord` on the Schema Registry contract | record to quarantine, never scored |
| Input | Frame size, brightness, contrast, sharpness, glare | `guardrails/input_checks.py: frame_checks` | band **NA**: the frame is reviewed in calendar order, as without AI |
| Input | Out of distribution: mean squared z-score of the 49 features against the training set, limit = training q99.5 (stored with the model as `ood`) | `input_checks.py: ood_check` | band **NA**, reason OOD |
| Input | Sensor sanity: stuck sensor in the last 3 days | `build_gold.py`, `input_checks.py: asset_checks` | asset abstains from IRE (not guessed) |
| Input | Asset not resolved by the asset master | `input_checks.py: asset_checks` | not scored; asset review queue |
| Output | Abstain band: severe probability within 0.05 of the operating threshold | `guardrails/output_policy.py` | band **UNCERTAIN**, "engineer review" |
| Output | Safety-critical floor: criticality A never below P2 on a score alone | `output_policy.py` | band raised to P2, event `safety_floor` |
| Output | No automated action | `output_policy.py`, app | response carries `action: engineer review required` and only `rank`, `suggest_inspection` |
| Process | KPI gate + non-regression vs the champion (TEST AUROC may drop at most 0.02) | `gate/kpi_gate.py` | chain stops, champion keeps serving, `GATE_FAILED` |
| Process | Feature hash lock | `feature_logic.py`, `predict.py` | endpoint refuses to start on a mismatch |
| Process | Drift: PSI of score and features, OOD and NA rates | `monitor/drift.py` | `DRIFT_ALERT`, guardrail event `drift_psi`, retrain trigger |

## Calibration of the frame checks

Tuned on the committed frame library (105 clean frames, 105 deliberately degraded ones) and
300 training images, choosing the limits that catch the most degraded frames while rejecting at
most 5 % of clean ones:

| Set | Rejected |
|---|---|
| clean library frames | 2 of 105 (both slightly soft: sharpness 0.005-0.006) |
| training images | 1 % |
| glare / low light | 100 % |
| blur | 72 % |
| occlusion / noise | mostly pass the frame checks: the OOD check and the planned frame-QC head are the line for those |

## Where to see them

- **Workbench → Asset 360**: every keyframe shows its band, the reason (NA / UNCERTAIN / safety
  floor) and an expander with each check's value and limit.
- **Workbench → Models & guardrails**: events by guardrail (`ref.guardrail_event`), latest
  events, the configured limits, drift metrics (`ref.model_drift`).
- **Endpoint**: the response carries `guardrails` (every check) and `guardrail_events` (the
  ones that fired); errors come back as `{"error": ...}` with the exception.
