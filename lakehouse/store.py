"""
The CAI side's door to the lakehouse: CDW Impala over HTTPS (impyla), the same connection as the
Workbench. Writes the MLOps record tables in <prefix>_ref (Iceberg):

  model_event      every training, gate, registration, deployment, promotion, retrain trigger
  guardrail_event  every guardrail that fired in batch scoring (monitor/drift.py)
  model_drift      PSI per feature and the OOD / NA rates per nightly run

Needs OGX_IMPALA_USER / OGX_IMPALA_PASSWORD (the CAI project environment). Writing never fails
the calling job: the model work stands, the record is best effort and says so in the job log.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "lakehouse.json").read_text())
PREFIX = os.environ.get("OGX_DB_PREFIX", CFG.get("db_prefix", "rsingh_ogx"))

TABLES = {
    "model_event": [
        ("event_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("model", "STRING"), ("event", "STRING"),
        ("stage", "STRING"), ("registry_name", "STRING"), ("registry_version", "INT"), ("model_version", "STRING"),
        ("estimator", "STRING"), ("git_sha", "STRING"), ("feature_version", "STRING"), ("feature_hash", "STRING"),
        ("threshold", "DOUBLE"), ("test_auroc", "DOUBLE"), ("test_sensitivity", "DOUBLE"),
        ("test_specificity", "DOUBLE"), ("test_brier", "DOUBLE"), ("train_rows", "INT"), ("feedback_rows", "INT"),
        ("gate_passed", "BOOLEAN"), ("gate_checks", "STRING"), ("mlflow_run_id", "STRING"),
        ("trigger_reason", "STRING"), ("approved_by", "STRING"), ("detail", "STRING")],
    "guardrail_event": [
        ("event_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("layer", "STRING"), ("guardrail", "STRING"),
        ("subject", "STRING"), ("value", "DOUBLE"), ("limit_rule", "STRING"), ("reason", "STRING"),
        ("model_version", "STRING"), ("source", "STRING")],
    "model_drift": [
        ("run_id", "STRING"), ("recorded_at", "TIMESTAMP"), ("model_version", "STRING"), ("metric", "STRING"),
        ("value", "DOUBLE"), ("status", "STRING"), ("n_reference", "INT"), ("n_current", "INT"), ("detail", "STRING")],
}


def model_version(meta: dict) -> str:
    """A readable, unique id for one trained candidate: estimator, features, commit, time."""
    if meta.get("model_version"):
        return meta["model_version"]
    t = str(meta.get("trained_at", ""))[:16].replace("-", "").replace(":", "").replace("T", "")
    return f"{meta.get('estimator', 'model')}-{meta['feature_version']}-{meta['git_sha'][:7]}-{t}"


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(float(v)) if isinstance(v, float) else str(v)
    if isinstance(v, datetime):
        return f"CAST('{v.strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
    if isinstance(v, date):
        return f"DATE '{v.isoformat()}'"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ImpalaStore:
    def __init__(self):
        from impala.dbapi import connect

        imp = CFG["impala"]
        self.conn = connect(host=os.environ.get("OGX_IMPALA_HOST", imp["host"]), port=int(imp["port"]), use_ssl=True,
                            use_http_transport=True, http_path=imp["http_path"], auth_mechanism=imp["auth_mechanism"],
                            user=os.environ["OGX_IMPALA_USER"], password=os.environ["OGX_IMPALA_PASSWORD"])

    @staticmethod
    def t(name: str) -> str:
        db, table = name.split(".") if "." in name else ("ref", name)
        return f"{PREFIX}_{db}.{table}"

    def execute(self, sql: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def query(self, sql: str) -> list[dict]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cols = [d[0].split(".")[-1] for d in cur.description or []]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            cur.close()

    def ensure(self, table: str) -> None:
        cols = ", ".join(f"{c} {t}" for c, t in TABLES[table])
        self.execute(f"CREATE TABLE IF NOT EXISTS {self.t(table)} ({cols}) STORED AS ICEBERG")

    def insert(self, table: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        self.ensure(table)
        cols = [c for c, _ in TABLES[table]]
        for i in range(0, len(rows), 200):
            values = ", ".join("(" + ", ".join(lit(r.get(c)) for c in cols) + ")" for r in rows[i:i + 200])
            self.execute(f"INSERT INTO {self.t(table)} ({', '.join(cols)}) VALUES {values}")
        return len(rows)


def publish(table: str, rows: list[dict], store: ImpalaStore | None = None) -> int:
    """Best effort: rows into <prefix>_ref.<table>; 0 (and a warning) when the lakehouse is unreachable."""
    if not rows:
        return 0
    if not os.environ.get("OGX_IMPALA_USER"):
        print(f"[lakehouse] no OGX_IMPALA_USER: {len(rows)} {table} rows kept locally only", flush=True)
        return 0
    try:
        n = (store or ImpalaStore()).insert(table, rows)
        print(f"[lakehouse] {n} rows -> {ImpalaStore.t(table)}", flush=True)
        return n
    except Exception as e:  # noqa: BLE001
        print(f"[lakehouse] WARNING {table} not published: {type(e).__name__}: {str(e)[:300]}", flush=True)
        return 0
