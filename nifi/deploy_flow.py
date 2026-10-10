"""
NiFi flow "OGX - SCADA Alarm Router" as code, on the federal-nifi Data Hub (NiFi 2, REST via Knox).

  python nifi/deploy_flow.py check       # resolve every property against NiFi's definitions, create nothing
  python nifi/deploy_flow.py deploy      # build the process group, enable services, start, version it
  python nifi/deploy_flow.py status      # queue and in/out counts per processor
  python nifi/deploy_flow.py export      # flow definition -> nifi/flows/ogx-scada-alarm-router.json
  python nifi/deploy_flow.py commit -m "..."   # new version in NiFi Registry (bucket ogx-flows)

The flow
  ConsumeKafka ogx.scada.alarm (JSON, parsed with the schema named after the topic in the Kafka
  cluster's Schema Registry)
    -> ValidateRecord against that contract; invalid records -> S3 quarantine
    -> QueryRecord: priority-1, HIHI and STUCK alarms -> PublishKafka ogx.alarm.priority
                    every valid alarm -> MergeRecord (5 min) -> S3 landing/nifi/scada_alarm/<date>/
  Failures queue in front of a stopped "Hold failures" processor so they stay visible on the canvas.

Only the OGX process group, its controller services and the ogx-flows bucket are created or
changed; the root-level truststore service is referenced, never modified. Credentials come from
OGX_WORKLOAD_USER / OGX_WORKLOAD_PASSWORD (sensitive NiFi properties, never printed).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "streaming.json").read_text())
NIFI = CFG["nifi"]
IN_TOPIC = CFG["topics"]["alarm"]["name"]
OUT_TOPIC = NIFI["out_topic"]["name"]
EXPORT = ROOT / "nifi" / "flows" / f"{NIFI['flow_name']}.json"


class Nifi:
    def __init__(self):
        self.s = requests.Session()
        self.s.auth = (os.environ["OGX_WORKLOAD_USER"], os.environ["OGX_WORKLOAD_PASSWORD"])
        self.api = NIFI["api"].rstrip("/")
        self.reg = NIFI["registry_api"].rstrip("/")
        self._types = {}
        self.dry = False   # check: resolve every property against the component definitions, create nothing
        self._n = 0

    def call(self, method, path, base=None, **kw):
        r = self.s.request(method, (base or self.api) + path, timeout=120, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:400]}")
        return r.json() if r.text.strip().startswith(("{", "[")) else r.text

    def bundle(self, kind: str, short: str) -> tuple[str, dict]:
        if kind not in self._types:
            key = "processorTypes" if kind == "processor" else "controllerServiceTypes"
            self._types[kind] = self.call("GET", f"/flow/{kind}-types")[key]
        hits = [t for t in self._types[kind] if t["type"].split(".")[-1] == short]
        if not hits:
            raise RuntimeError(f"{kind} type {short} not installed")
        return hits[0]["type"], hits[0]["bundle"]

    def definition(self, kind: str, short: str) -> dict:
        typ, b = self.bundle(kind, short)
        return self.call("GET", f"/flow/{kind}-definition/{b['group']}/{b['artifact']}/{b['version']}/{typ}")

    def fake_id(self) -> str:
        self._n += 1
        return f"dry-{self._n}"

    @staticmethod
    def map_props(descriptors: dict, wanted: dict) -> dict:
        """Accept property names or display names; fail loudly on an unknown one."""
        by_display = {d["displayName"]: n for n, d in descriptors.items()}
        out = {}
        for k, v in wanted.items():
            name = k if k in descriptors else by_display.get(k)
            if name is None and not k.startswith("+"):
                raise RuntimeError(f"unknown property {k!r}; known: {sorted(by_display)}")
            out[name or k[1:]] = v   # '+name' = a dynamic property
        return out

    # ---- process group, services, processors, connections
    def root_id(self) -> str:
        return self.call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]

    def find_group(self, name: str) -> dict | None:
        groups = self.call("GET", f"/flow/process-groups/{self.root_id()}")["processGroupFlow"]["flow"]["processGroups"]
        return next((g for g in groups if g["component"]["name"] == name), None)

    def service_id(self, pg: str, name: str) -> str:
        if self.dry:
            pg = self.root_id()
        cs = self.call("GET", f"/flow/process-groups/{pg}/controller-services?includeAncestorGroups=true")
        hits = [c["id"] for c in cs["controllerServices"] if c["component"]["name"] == name]
        if not hits:
            raise RuntimeError(f"controller service {name!r} not visible from the process group")
        return hits[0]

    def add_service(self, pg: str, short: str, name: str, props: dict) -> str:
        typ, bundle = self.bundle("controller-service", short)
        mapped = self.map_props(self.definition("controller-service", short)["propertyDescriptors"], props)
        if self.dry:
            print(f"  ok service {name}: {sorted(mapped)}", flush=True)
            return self.fake_id()
        ent = self.call("POST", f"/process-groups/{pg}/controller-services",
                        json={"revision": {"version": 0}, "component": {"type": typ, "bundle": bundle, "name": name}})
        ent = self.call("PUT", f"/controller-services/{ent['id']}", json={
            "revision": ent["revision"], "component": {"id": ent["id"], "properties": mapped}})
        print(f"  service {name} ({short})", flush=True)
        return ent["id"]

    def enable_services(self, pg: str) -> None:
        if self.dry:
            return
        self.call("PUT", f"/flow/process-groups/{pg}/controller-services",
                  json={"id": pg, "state": "ENABLED"})
        for _ in range(30):
            cs = self.call("GET", f"/flow/process-groups/{pg}/controller-services")["controllerServices"]
            own = [c for c in cs if c["component"]["parentGroupId"] == pg]
            states = {c["component"]["name"]: c["component"]["state"] for c in own}
            if all(s == "ENABLED" for s in states.values()):
                print(f"  services enabled: {len(states)}", flush=True)
                return
            time.sleep(2)
        bad = {c["component"]["name"]: c["component"].get("validationErrors") for c in own
               if c["component"]["state"] != "ENABLED"}
        raise RuntimeError(f"services not enabled: {bad}")

    def add_processor(self, pg: str, short: str, name: str, pos, props: dict, auto_terminate=(),
                      schedule: str | None = None) -> str:
        typ, bundle = self.bundle("processor", short)
        defn = self.definition("processor", short)
        mapped = self.map_props(defn["propertyDescriptors"], props)
        rels = {r["name"] for r in defn.get("supportedRelationships") or []}
        unknown = set(auto_terminate) - rels - {"original"}
        if unknown:
            raise RuntimeError(f"{name}: unknown relationships {unknown}; known {sorted(rels)}")
        if self.dry:
            print(f"  ok processor {name}: {sorted(mapped)} rels {sorted(rels)}", flush=True)
            return self.fake_id()
        ent = self.call("POST", f"/process-groups/{pg}/processors", json={
            "revision": {"version": 0},
            "component": {"type": typ, "bundle": bundle, "name": name, "position": {"x": pos[0], "y": pos[1]}}})
        config = {"properties": mapped,
                  "autoTerminatedRelationships": list(auto_terminate)}
        if schedule:
            config["schedulingPeriod"] = schedule
        self.call("PUT", f"/processors/{ent['id']}", json={
            "revision": ent["revision"], "component": {"id": ent["id"], "config": config}})
        print(f"  processor {name} ({short})", flush=True)
        return ent["id"]

    def connect(self, pg: str, src: str, dst: str, rels: list[str]) -> None:
        if self.dry:
            return
        self.call("POST", f"/process-groups/{pg}/connections", json={
            "revision": {"version": 0},
            "component": {"source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
                          "destination": {"id": dst, "groupId": pg, "type": "PROCESSOR"},
                          "selectedRelationships": rels,
                          "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}})

    def run(self, proc_id: str, state: str) -> None:
        if self.dry:
            return
        ent = self.call("GET", f"/processors/{proc_id}")
        if ent["component"].get("validationErrors"):
            raise RuntimeError(f"{ent['component']['name']} invalid: {ent['component']['validationErrors']}")
        self.call("PUT", f"/processors/{proc_id}/run-status", json={"revision": ent["revision"], "state": state})

    # ---- NiFi Registry
    def bucket_id(self) -> str:
        buckets = self.call("GET", "/buckets", base=self.reg)
        hit = next((b for b in buckets if b["name"] == NIFI["registry_bucket"]), None)
        if hit is None:
            hit = self.call("POST", "/buckets", base=self.reg, json={
                "name": NIFI["registry_bucket"], "description": "OGX asset-integrity NiFi flows (flow-as-code)"})
            print(f"  registry bucket {NIFI['registry_bucket']} created", flush=True)
        return hit["identifier"]

    def registry_client_id(self) -> str:
        regs = self.call("GET", "/flow/registries")["registries"]
        return next(r["id"] for r in regs if r["component"]["name"] == NIFI["registry_client"])

    def commit(self, pg: str, comment: str) -> int:
        ent = self.call("GET", f"/process-groups/{pg}")
        vci = ent["component"].get("versionControlInformation")
        body = {"processGroupRevision": ent["revision"], "versionedFlow": {
            "registryId": self.registry_client_id(), "bucketId": self.bucket_id(),
            "flowName": NIFI["flow_name"], "description": "OGX SCADA alarm router (nifi/deploy_flow.py)",
            "comments": comment, "action": "COMMIT"}}
        if vci:
            body["versionedFlow"]["flowId"] = vci["flowId"]
        out = self.call("POST", f"/versions/process-groups/{pg}", json=body)
        version = out["versionControlInformation"]["version"]
        print(f"  NiFi Registry: {NIFI['registry_bucket']}/{NIFI['flow_name']} version {version}", flush=True)
        return version


def build(n: Nifi) -> str:
    if n.find_group(NIFI["process_group"]):
        raise SystemExit(f"process group {NIFI['process_group']!r} already exists: use status / commit "
                         "(replacing it is a delete and needs approval)")
    user, password = os.environ["OGX_WORKLOAD_USER"], os.environ["OGX_WORKLOAD_PASSWORD"]
    x, y = NIFI["position"]
    pg = "dry-pg" if n.dry else n.call("POST", f"/process-groups/{n.root_id()}/process-groups", json={
        "revision": {"version": 0},
        "component": {"name": NIFI["process_group"], "position": {"x": x, "y": y},
                      "comments": "Kafka ogx.scada.alarm -> Schema Registry contract -> priority topic + S3 "
                                  "landing. Built by nifi/deploy_flow.py in partomia/O-G-Asset-Integrity-Lakehouse."}})["id"]
    print(f"process group {NIFI['process_group']} {pg}", flush=True)

    ssl = n.service_id(pg, NIFI["ssl_context_service"])
    sr = n.add_service(pg, "ClouderaSchemaRegistry", "OGX Schema Registry (Kafka)", {
        "Schema Registry URL": NIFI["schema_registry_url"], "Basic Authentication Username": user,
        "Basic Authentication Password": password, "SSL Context Service": ssl})
    reader = n.add_service(pg, "JsonTreeReader", "OGX JSON reader (Schema Registry)", {
        "Schema Access Strategy": "schema-name", "Schema Registry": sr, "Schema Name": "${kafka.topic}"})
    writer = n.add_service(pg, "JsonRecordSetWriter", "OGX JSON writer", {
        "Schema Write Strategy": "no-schema", "Schema Access Strategy": "inherit-record-schema",
        "Output Grouping": "output-oneline"})
    kafka = n.add_service(pg, "Kafka3ConnectionService", "OGX Kafka (SASL_SSL PLAIN)", {
        "Bootstrap Servers": NIFI["kafka_bootstrap"], "Security Protocol": "SASL_SSL",
        "SASL Mechanism": "PLAIN", "SASL Username": user, "SASL Password": password,
        "SSL Context Service": ssl})
    krb = n.add_service(pg, "KerberosPasswordUserService", "OGX Kerberos user (object store)", {
        "Kerberos Principal": user, "Kerberos Password": password})
    n.enable_services(pg)

    consume = n.add_processor(pg, "ConsumeKafka", f"Consume {IN_TOPIC}", (0, 0), {
        "Kafka Connection Service": kafka, "Group ID": NIFI["consumer_group"], "Topics": IN_TOPIC,
        "auto.offset.reset": "earliest", "Processing Strategy": "RECORD",
        "Record Reader": reader, "Record Writer": writer})
    validate = n.add_processor(pg, "ValidateRecord", "Validate against the Schema Registry contract", (0, 250), {
        "Record Reader": reader, "Record Writer": writer, "Allow Extra Fields": "false",
        "Strict Type Checking": "true"})
    route = n.add_processor(pg, "QueryRecord", "Route priority alarms", (0, 500), {
        "Record Reader": reader, "Record Writer": writer, "Include Zero Record FlowFiles": "false",
        "+priority": f"SELECT * FROM FLOWFILE WHERE {NIFI['priority_rule']}",
        "+all": "SELECT * FROM FLOWFILE"}, auto_terminate=["original"])
    publish = n.add_processor(pg, "PublishKafka", f"Publish {OUT_TOPIC}", (-450, 750), {
        "Kafka Connection Service": kafka, "Topic Name": OUT_TOPIC, "Record Reader": reader,
        "Record Writer": writer, "Message Key Field": "sensor_tag"}, auto_terminate=["success"])
    merge = n.add_processor(pg, "MergeRecord", "Batch alarms (5 minutes)", (0, 750), {
        "Record Reader": reader, "Record Writer": writer, "Minimum Number of Records": "1",
        "Maximum Number of Records": "100000", "Max Bin Age": "5 min"}, auto_terminate=["original"])
    name = n.add_processor(pg, "UpdateAttribute", "Name the landing file", (0, 1000), {
        "+filename": "scada_alarm_${now():format('yyyyMMdd_HHmmss')}_${UUID()}.jsonl"})
    land = n.add_processor(pg, "PutCDPObjectStore", "Land to S3 (rsingh_ogx/landing/nifi)", (0, 1250), {
        "Directory": NIFI["landing_dir"] + "/${now():format('yyyy-MM-dd')}",
        "Kerberos User Service": krb}, auto_terminate=["success"])
    qname = n.add_processor(pg, "UpdateAttribute", "Name the quarantine file", (450, 500), {
        "+filename": "invalid_${now():format('yyyyMMdd_HHmmss')}_${UUID()}.jsonl"})
    quarantine = n.add_processor(pg, "PutCDPObjectStore", "Quarantine to S3", (450, 750), {
        "Directory": NIFI["quarantine_dir"] + "/${now():format('yyyy-MM-dd')}",
        "Kerberos User Service": krb}, auto_terminate=["success"])
    hold = n.add_processor(pg, "UpdateAttribute", "Hold failures (inspect, then replay)", (900, 750), {})

    n.connect(pg, consume, validate, ["success"])
    n.connect(pg, consume, qname, ["parse failure"])
    n.connect(pg, validate, route, ["valid"])
    n.connect(pg, validate, qname, ["invalid"])
    n.connect(pg, validate, hold, ["failure"])
    n.connect(pg, route, publish, ["priority"])
    n.connect(pg, route, merge, ["all"])
    n.connect(pg, route, hold, ["failure"])
    n.connect(pg, publish, hold, ["failure"])
    n.connect(pg, merge, name, ["merged"])
    n.connect(pg, merge, hold, ["failure"])
    n.connect(pg, name, land, ["success"])
    n.connect(pg, land, hold, ["failure"])
    n.connect(pg, qname, quarantine, ["success"])
    n.connect(pg, quarantine, hold, ["failure"])
    if n.dry:
        print("check passed: every property and relationship resolves")
        return pg
    n.call("PUT", f"/processors/{hold}", json={
        "revision": n.call("GET", f"/processors/{hold}")["revision"],
        "component": {"id": hold, "config": {"autoTerminatedRelationships": ["success"]}}})

    for p in (land, quarantine, name, qname, merge, publish, route, validate, consume):   # sinks first
        n.run(p, "RUNNING")
    print("  processors running (Hold failures stays stopped)", flush=True)
    return pg


def status(n: Nifi) -> list[dict]:
    g = n.find_group(NIFI["process_group"])
    if not g:
        print("not deployed")
        return []
    procs = n.call("GET", f"/flow/process-groups/{g['id']}/status?recursive=false")
    rows = []
    for p in procs["processGroupStatus"]["aggregateSnapshot"]["processorStatusSnapshots"]:
        s = p["processorStatusSnapshot"]
        rows.append({"processor": s["name"], "state": s["runStatus"], "in_5m": s["flowFilesIn"],
                     "out_5m": s["flowFilesOut"], "queued": s.get("queued", "")})
    vci = n.call("GET", f"/process-groups/{g['id']}")["component"].get("versionControlInformation") or {}
    print(f"{NIFI['process_group']}: registry version {vci.get('version')} ({vci.get('state')})")
    for r in sorted(rows, key=lambda r: r["processor"]):
        print(f"  {r['state']:8} in {r['in_5m']:>6} out {r['out_5m']:>6}  {r['processor']}")
    return rows


def export(n: Nifi) -> Path:
    g = n.find_group(NIFI["process_group"])
    flow = n.call("GET", f"/process-groups/{g['id']}/download?includeReferencedServices=true")
    EXPORT.parent.mkdir(parents=True, exist_ok=True)
    EXPORT.write_text(json.dumps(flow, indent=2, sort_keys=True) + "\n")
    print(f"exported -> {EXPORT.relative_to(ROOT)} (sensitive values are never included)")
    return EXPORT


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["check", "deploy", "status", "export", "commit"])
    ap.add_argument("-m", "--message", default="")
    a = ap.parse_args(argv)
    n = Nifi()
    if a.action == "check":
        n.dry = True
        build(n)
    elif a.action == "deploy":
        pg = build(n)
        n.commit(pg, a.message or "initial version: consume, validate, route, land")
        export(n)
    elif a.action == "status":
        status(n)
    elif a.action == "export":
        export(n)
    else:
        n.commit(n.find_group(NIFI["process_group"])["id"], a.message or "update")
    return 0


if __name__ == "__main__":
    sys.exit(main())
