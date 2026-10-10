# NiFi: SCADA alarm routing with Schema Registry and NiFi Registry

Process group **OGX - SCADA Alarm Router** on the `federal-nifi` Data Hub (NiFi 2.6), built as
code by `nifi/deploy_flow.py` and versioned in **NiFi Registry** (bucket `ogx-flows`, flow
`ogx-scada-alarm-router`). The exported definition is committed at
`nifi/flows/ogx-scada-alarm-router.json` (NiFi never exports sensitive values).

```
ConsumeKafka ogx.scada.alarm ──► ValidateRecord ──valid──► QueryRecord ──priority──► PublishKafka ogx.alarm.priority
 (JSON, schema "${kafka.topic}"     │ against the              │ priority = '1' OR
  from Schema Registry)             │ registered contract      │ alarm_type IN (HIHI, STUCK)
        │ parse failure             │ invalid                  └──all──► MergeRecord (5 min) ─► PutCDPObjectStore
        └───────────────────────────┴──► PutCDPObjectStore quarantine/      s3a://.../rsingh_ogx/landing/nifi/scada_alarm/<date>/
  every failure ─► "Hold failures" (stopped: the queue stays visible on the canvas for replay)
```

| Piece | Service |
|---|---|
| Message contract | Kafka cluster **Schema Registry**: `ogx.scada.alarm` (Avro, registered by `stream/producer/topics.py`); `ogx.alarm.priority` registered with the same contract |
| Reader / writer | `JsonTreeReader` with `ClouderaSchemaRegistry` (schema name = topic), `JsonRecordSetWriter` |
| Kafka | `Kafka3ConnectionService`, SASL_SSL PLAIN with the workload user, the root truststore-only SSL context service (referenced, not changed) |
| S3 | `PutCDPObjectStore` with a Kerberos user service (workload user; IDBroker maps it to the bucket role) |
| Versioning | NiFi Registry client "Default NiFi Registry Client", bucket `ogx-flows` |

Why NiFi here: the CDE Spark stream ingests every alarm into bronze; NiFi does the low-latency
routing a control room needs (priority alarms on their own topic within seconds) and a raw
landing copy in the object store, with schema validation at the edge and no code to deploy.
`ogx.alarm.priority` is kept out of `config/streaming.json` `topics`, so the bronze stream does
not ingest the routed copies twice.

## Commands

```bash
set -a; source .env; set +a
python nifi/deploy_flow.py check                  # resolve every property against NiFi's definitions
python nifi/deploy_flow.py deploy                 # build, enable, start, version 1, export
python nifi/deploy_flow.py status                 # per-processor counts (5-minute window)
python nifi/deploy_flow.py commit -m "..."        # a new version in NiFi Registry after a canvas change
python nifi/deploy_flow.py export                 # refresh nifi/flows/*.json
```

First run (2026-10-11): 84 alarms consumed from the topic backlog, 84 valid and landed in
`landing/nifi/scada_alarm/2026-10-10/` (two files), 6 priority alarms published, 0 quarantined.
