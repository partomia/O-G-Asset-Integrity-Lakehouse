# OGX Agentic Integrity Monitor

A second CAI application beside the Integrity Workbench: three agents on Cloudera AI Inference
read the lakehouse for one facility and business date, explain what is abnormal, assess
integrity with the inspection evidence, and draft work tickets for an engineer to approve.

It follows the Cloudera AMP "Agentic Oil and Gas Well Monitoring"
(`git.schuler.me/thesneakernet/amp-oilandgas-agenticwellmonitoring`, Apache 2.0): the anomaly
pre-screen before any token is spent, the three-stage agent chain that streams and passes each
stage's report to the next, the fenced ticket blocks and their parser, the OpenAI-compatible CAII
client with retry, circuit breaker and cache, and the historical-pattern search. What differs:

| AMP | Here |
|---|---|
| 4 sites x 6 wells, seeded random telemetry in SQLite | facilities FLD1 (12 wells), PL03 (60 segments), REF1 (refinery) from the lakehouse: certified worklist, streaming 15-minute windows, gold sensor facts, inspections, report chunks, work orders |
| FastAPI + React + Node supervisor | Streamlit, one process, 1 vCPU / 4 GB (`agent_app/`) |
| Tickets land in SQLite as open | Draft tickets in `ref.agent_ticket`; an engineer approves or rejects (`ref.agent_ticket_review`, append-only); nothing is raised automatically |
| No output checks | LLM guardrail (`agents/grounding.py`): tags, numbers and report citations checked against each stage's context; tickets scoped, abstained assets and forbidden actions flagged; events in `ref.guardrail_event` |
| Pinecone / in-memory vectors | TF-IDF over ~800 report chunks and past analyses (no vector store needed at this size) |
| LLM down = error | Rule-based stages from the same data (`agents/offline.py`), labelled as such |

## Flow

1. **Pre-screen** (`agents/prescreen.py`): rolling z-score over the 15-minute window means (24
   windows = 6 h, |z| >= 3 and >= 10 % off the mean); stuck, breach and unit-change counts from
   `gold.fact_sensor_daily`, the certified layer behind IRE. Quick depth with nothing flagged stops
   here: zero tokens.
2. **Condition analyst**: facility condition, flagged assets, data faults (stuck = abstained, bar
   readings = unit change), data gaps.
3. **Integrity engineer**: immediate risk, watchlist, sensor faults; cites `[file p.N]` of the
   report excerpts it was given; refers to open work orders instead of duplicating them.
4. **Operations planner**: executive summary and at most 8 ` ```ticket ` blocks (Inspection,
   Repair, Sensor, Monitor).
5. **Guardrails, records**: tickets outside the facility are dropped; every analysis, ticket and
   fired guardrail is written to `rsingh_ogx_ref`.

Prompts: `agents/config/agents.yaml`, `agents/config/tasks.yaml`. Settings: `config/inference.json`
(endpoint, pre-screen, scope) and `config/guardrails.yaml` `llm`.

## Planted faults it should find

| Facility, date | Signal | Expected ticket |
|---|---|---|
| REF1, 10-05 / 10-06 | VI-112B stuck (P-112B abstained) | Sensor on P-112B |
| REF1, 10-06 | PI-105A in bar from 10:00 | Sensor on P-105A |
| PL03, any | CP-PL03-027 wall-loss rate above threshold all day; 38.7 % wall loss, pitting, CUI | High Inspection/Repair on PL-03-SEG-027 |

Found while building it: silver `sensor_window` rows of 10-04 .. 10-09 carry a 1450 psi high
threshold on the well pressure sensors (wells run at ~1800 psi), so every window is a "breach";
gold counts 0 and the 10-23+ windows use 2500 psi. The pre-screen therefore takes flag counts from
gold. Re-running `stream-agg` for those dates would clean silver.

## Cloudera AI Inference

Status 2026-11-02: torn down (see docs/PROJECT_LOG.md); the resources below are what was built
and how to rebuild them. Until then the app runs rule-based.

| Resource | Name | Notes |
|---|---|---|
| Compute cluster | `rsingh-ogx-compute` | `cdp compute create-cluster`, the environment's three worker subnets |
| Inference service | `rsingh-ogx-inference` | 3 x m5.xlarge `mlinfra` (always on), CPU group m5.xlarge 0-2, GPU group g4dn.2xlarge 0-1 (1 x T4 16 GB); public load balancer, JWT on every call |
| AI Registry model | `ogx-llm-qwen2-5-7b-instruct-awq` v1 | Hugging Face `Qwen/Qwen2.5-7B-Instruct-AWQ` (Apache 2.0), vLLM, 4-bit AWQ (~5.6 GB) on one T4; `scripts/caii_ctl.py import` |
| Endpoint | `ogx-llm` | 1 GPU, 4 vCPU / 24 GiB, 1 replica; `scripts/caii_ctl.py delete` frees the GPU node (scaling to 0 leaves the old revision holding it) |

```bash
set -a; source .env; set +a
python scripts/caii_ctl.py status
python scripts/caii_ctl.py deploy && python scripts/caii_ctl.py wait
python scripts/caii_ctl.py chat "Reply with OK."
python ci/setup_cai.py --agent-app          # the CAI application
```

The app authenticates to CAII with the JWT CAI mounts at `/tmp/jwt`; on a laptop
`agents/llm.py` asks the cdp CLI for a workload token. `OGX_CAII_BASE_URL` / `OGX_CAII_MODEL`
override `config/inference.json`.

Payloads used to create the service (once):

```json
{"environment": "federal-cdp-env", "name": "rsingh-ogx-compute",
 "network": {"subnets": ["subnet-01746fef14b00fc04", "subnet-0ee59598c893c6707", "subnet-0e3619cb857148b5d"]}}
```

```json
{"appName": "rsingh-ogx-inference", "clusterCrn": "<rsingh-ogx-compute CRN>",
 "provisionK8sRequest": {"instanceGroups": [
   {"instanceType": "m5.xlarge", "instanceCount": 0, "autoscaling": {"minInstances": 0, "maxInstances": 2, "enabled": true}}]},
 "usePublicLoadBalancer": true, "loadBalancerIPWhitelists": ["0.0.0.0/0"]}
```

then the GPU group, `cdp ml add-instance-groups-ml-serving-app`:

```json
{"appCrn": "<rsingh-ogx-inference CRN>", "instanceGroups": [{"instanceType": "g4dn.2xlarge", "instanceCount": 0,
  "autoscaling": {"minInstances": 0, "maxInstances": 1, "enabled": true}, "accelerator": {"type": "nvidia-gpu", "count": 1}}]}
```

Lessons from the first attempts, each failing after 5-10 minutes:

- g5 and g6 do not provision here; g4dn does. g5.2xlarge and g6.2xlarge failed Liftie's zone
  check across the three subnets, and g6.2xlarge pinned to each single subnet failed in the AWS
  provisioner (no detail without AWS access). The T4 has no bf16 and 16 GB, hence the AWQ
  build of Qwen2.5-7B; the unquantised `ogx-llm-qwen2-5-7b-instruct` stays in the registry for a
  larger GPU. The service status only said "Failed
  to create Instance type of m5.2xlarge" (the CPU type); the real cause was in
  `cdp compute describe-cluster` `message`: "mlgpu-... does not have any instance type available
  across all of its subnet availability zones ... g5.2xlarge". A service that fails to provision
  has to be deleted and created again.
- Create the service with the CPU group only, then add GPU groups: a bad GPU type then fails the
  addition, not the service. Audit log: `cdp ml get-audit-events --resource-crn <appCrn>`, then
  `cdp ml get-logs --request-id <id> --workspace-crn <appCrn>`.

## How to see it working

- `pytest tests/test_agents.py`: pre-screen, ticket parser, every LLM guardrail firing, the runner
  with a scripted LLM (stage chaining, fallback on failure, short circuit), records, SSE parsing.
- App, REF1 on 2026-10-06, depth deep: Sensor tickets for P-112B and P-105A; PL03: High ticket on
  PL-03-SEG-027 citing its report; LLM & guardrails tab: tokens per analysis and fired guardrails.
