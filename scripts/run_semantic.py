"""
Semantic layer: certified KPI views, MIS views, dashboard views and the KPI consistency check,
from the SQL in sql/semantic/. Copied from GDL's run_semantic.py; one set of SQL runs on two
engines:

  --engine impala   CDW Impala (Atlas records column lineage for the views). Needs
                    OGX_IMPALA_USER and OGX_IMPALA_PASSWORD.
  --engine spark    local Spark + Iceberg (scripts/run_local.py semantic, and CI)

Steps:
  views   (re)create the views in rsingh_ogx_semantic (once per run)
  check   every consumer (MIS views, a direct gold query) must give the certified view's figure;
          results go to ref.recon_results (layer 'semantic')
  adhoc   print the risk worklist, coverage and time-to-review for the last date

SQL is written with the default database names (rsingh_ogx_*) so it pastes into Hue as is;
--db-prefix rewrites them.

Usage:
  python scripts/run_semantic.py --engine spark --warehouse /tmp/w --dates 2026-10-07,2026-10-08
  set -a; source .env; set +a
  python scripts/run_semantic.py --engine impala --steps views,check,adhoc
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"
DEFAULT_PREFIX = "rsingh_ogx"
STEPS = ("views", "load", "check", "adhoc")
TOLERANCE = 0.005


# ---------------------------------------------------------------- engines


class SparkEngine:
    name = "spark"
    now = "localtimestamp()"

    def __init__(self, spark):
        self.spark = spark

    def execute(self, sql: str) -> None:
        self.spark.sql(sql)

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        df = self.spark.sql(sql)
        return df.columns, [tuple(r) for r in df.collect()]

    def dialect(self, sql: str) -> str:
        return sql


class ImpalaEngine:
    name = "impala"
    now = "utc_timestamp()"

    def __init__(self, cfg: dict):
        from impala.dbapi import connect

        user, password = os.environ.get("OGX_IMPALA_USER"), os.environ.get("OGX_IMPALA_PASSWORD")
        if not user or not password:
            raise SystemExit("set OGX_IMPALA_USER and OGX_IMPALA_PASSWORD (CDP workload user) for --engine impala")
        self.conn = connect(host=os.environ.get("OGX_IMPALA_HOST", cfg["host"]), port=int(cfg["port"]),
                            use_ssl=True, use_http_transport=True, http_path=cfg["http_path"],
                            auth_mechanism=cfg["auth_mechanism"], user=user, password=password)

    def execute(self, sql: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def query(self, sql: str) -> tuple[list[str], list[tuple]]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            cols = [d[0].split(".")[-1] for d in cur.description or []]
            return cols, [tuple(r) for r in cur.fetchall()] if cur.description else []
        finally:
            cur.close()

    def dialect(self, sql: str) -> str:
        return sql


# ---------------------------------------------------------------- SQL files


def statements(text: str) -> list[str]:
    """Split a SQL file into statements: comment lines dropped, ';' at a line end ends one."""
    out, cur = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        cur.append(line)
        if line.rstrip().endswith(";"):
            out.append("\n".join(cur).strip().rstrip(";").strip())
            cur = []
    rest = "\n".join(cur).strip()
    return [s for s in out + ([rest] if rest else []) if s]


class Renderer:
    def __init__(self, engine, prefix: str):
        self.engine, self.prefix = engine, prefix

    def __call__(self, sql: str, params: dict | None = None) -> str:
        if self.prefix != DEFAULT_PREFIX:
            sql = sql.replace(f"{DEFAULT_PREFIX}_", f"{self.prefix}_")
        for k, v in (params or {}).items():
            sql = sql.replace("${" + k + "}", str(v))
        left = sorted(set(re.findall(r"\$\{(\w+)\}", sql)))
        if left:
            raise ValueError(f"no value for {left}")
        return self.engine.dialect(sql)


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(float(v)) if isinstance(v, float) else str(v)
    return "'" + str(v).replace("\\", "").replace("'", "") + "'"


def num(v) -> float:
    return float(v) if v is not None else 0.0


# ---------------------------------------------------------------- runner


class Semantic:
    def __init__(self, engine, prefix: str = DEFAULT_PREFIX, pipeline_run: str | None = None):
        self.e, self.prefix = engine, prefix
        self.r = Renderer(engine, prefix)
        self.run_id = f"semantic-{uuid.uuid4().hex[:12]}"
        self.pipeline_run = pipeline_run or self.run_id

    def t(self, layer: str, name: str) -> str:
        return f"{self.prefix}_{layer}.{name}"

    def row(self, sql: str) -> tuple:
        rows = self.e.query(self.r(sql))[1]
        return rows[0] if rows else ()

    def views(self) -> None:
        self.e.execute(f"CREATE DATABASE IF NOT EXISTS {self.prefix}_semantic")
        for f in sorted((SQL / "semantic").glob("*.sql")):
            for s in statements(f.read_text()):
                self.e.execute(self.r(s))
            print(f"semantic: {f.name}", flush=True)

    def check(self, d: date) -> list[tuple]:
        """Each consumer must reproduce the certified view's figure for the date."""
        on = f"WHERE business_date = DATE '{d.isoformat()}'"
        sem = lambda v: self.t("semantic", v)  # noqa: E731
        rows = []

        def add(entity, consumer, expected, actual, detail):
            x, a = num(expected), num(actual)
            diff = a - x
            ok = abs(diff) <= TOLERANCE * max(1.0, abs(x))
            rows.append((entity, consumer, x, a, diff, "MATCHED" if ok else "MISMATCH", detail))

        ire = self.row(f"SELECT SUM(exposure), COUNT(*), SUM(is_abstained) FROM {sem('kpi_integrity_risk_exposure')} {on}")
        m = self.row(f"SELECT SUM(exposure), SUM(assets), SUM(abstained_assets) FROM {sem('mis_risk_by_facility')} {on}")
        add("IRE", "mis_risk_by_facility.exposure", ire[0], m[0], "sum over facilities")
        add("IRE", "mis_risk_by_facility.assets", ire[1], m[1], "assets")
        add("IRE", "mis_risk_by_facility.abstained", ire[2], m[2], "abstained assets")
        w = self.row(f"SELECT SUM(exposure) FROM {sem('mis_risk_worklist')} {on}")
        add("IRE", "mis_risk_worklist.exposure", ire[0], w[0], "worklist total")
        g = self.row(f"SELECT COUNT(*) FROM {self.t('gold', 'fact_asset_risk_daily')} {on}")
        add("IRE", "gold.fact_asset_risk_daily.assets", ire[1], g[0], "every gold risk row is in the KPI")

        unc = self.row(f"SELECT SUM(is_received), SUM(is_covered) FROM {sem('kpi_unstructured_coverage')} {on}")
        m = self.row(f"SELECT SUM(received), SUM(covered) FROM {sem('mis_coverage_by_format')} {on}")
        add("UNC", "mis_coverage_by_format.received", unc[0], m[0], "objects received")
        add("UNC", "mis_coverage_by_format.covered", unc[1], m[1], "objects extracted and linked")

        ttr = self.row(f"SELECT COUNT(*), SUM(hours_to_review) FROM {sem('kpi_severe_time_to_review')} {on}")
        m = self.row(f"SELECT SUM(severe_defects), SUM(avg_hours * severe_defects) FROM {sem('mis_time_to_review')} {on}")
        add("TTR", "mis_time_to_review.severe_defects", ttr[0], m[0], "severe defects x 2 orders")
        add("TTR", "mis_time_to_review.hours", ttr[1], m[1], "total hours")

        self.write_results(d, rows)
        bad = [x for x in rows if x[5] != "MATCHED"]
        cov = num(unc[1]) / num(unc[0]) if num(unc[0]) else 0.0
        print(f"kpi consistency {d}: {len(rows) - len(bad)} MATCHED, {len(bad)} MISMATCH "
              f"(exposure {num(ire[0]):,.2f} over {int(num(ire[1]))} assets, {int(num(ire[2]))} abstained; "
              f"coverage {cov:.1%}; {int(num(ttr[0])) // 2} severe defects)", flush=True)
        for x in bad:
            print(f"  MISMATCH {x[0]} {x[1]}: expected {x[2]}, got {x[3]} ({x[6]})", flush=True)
        return bad

    def write_results(self, d: date, rows: list[tuple]) -> None:
        """Replace the date's 'semantic' rows in ref.recon_results in one commit: overwrite the
        partition with its other rows plus the new ones (Impala has no DELETE on copy-on-write)."""
        t, bid = self.t("ref", "recon_results"), "B" + d.strftime("%Y%m%d")
        new = " UNION ALL ".join(
            f"SELECT {lit(self.run_id)}, {lit(bid)}, DATE '{d.isoformat()}', 'semantic', {lit(e)}, {lit(c)}, "
            f"CAST({lit(x)} AS DOUBLE), CAST({lit(a)} AS DOUBLE), CAST({lit(df)} AS DOUBLE), {lit(s)}, {lit(det)}, "
            f"{self.e.now}" for e, c, x, a, df, s, det in rows)
        self.e.execute(f"INSERT OVERWRITE TABLE {t} SELECT * FROM {t} WHERE business_date = DATE '{d.isoformat()}' "
                       f"AND layer <> 'semantic' UNION ALL {new}")

    def adhoc(self, d: date) -> None:
        on = f"WHERE business_date = DATE '{d.isoformat()}'"
        sem = lambda v: self.t("semantic", v)  # noqa: E731
        show("risk worklist (top 10)", *self.e.query(
            f"SELECT risk_rank, tag, asset_class, facility_id, criticality, wall_loss_pct, rule_score, risk_band, "
            f"exposure, abstain_reason FROM {sem('mis_risk_worklist')} {on} AND risk_rank <= 10 ORDER BY risk_rank"))
        show("coverage by format", *self.e.query(
            f"SELECT object_format, received, extracted, covered, coverage FROM {sem('mis_coverage_by_format')} {on} ORDER BY object_format"))
        show("severe-defect time to review", *self.e.query(
            f"SELECT worklist_order, severe_defects, avg_hours, max_hours FROM {sem('mis_time_to_review')} {on} "
            f"ORDER BY worklist_order"))


def show(name: str, cols: list[str], rows: list[tuple], limit: int = 12) -> None:
    print(f"\n-- {name}: {len(rows)} row(s)")
    if not rows:
        return
    cells = [[("" if v is None else (f"{v:.2f}" if isinstance(v, float) else str(v)))[:40] for v in r]
             for r in rows[:limit]]
    widths = [max(len(c), *(len(r[i]) for r in cells)) for i, c in enumerate(cols)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    for r in cells:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit} more")


def run(engine, prefix: str, dates: list[date], steps=("views", "check"), pipeline_run=None,
        fail_on_mismatch: bool = False) -> int:
    sem = Semantic(engine, prefix, pipeline_run)
    if "views" in steps:
        sem.views()
    bad = 0
    for d in dates:
        if "check" in steps:
            bad += len(sem.check(d))
    if "adhoc" in steps:
        sem.adhoc(dates[-1])
    if bad and fail_on_mismatch:
        raise RuntimeError(f"{bad} KPI consistency mismatch(es)")
    return bad


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--engine", choices=("spark", "impala"), default="spark")
    p.add_argument("--dates", default=None, help="comma-separated business dates (default: the pipeline's)")
    p.add_argument("--steps", default="views,check")
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    p.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"), help="local Iceberg warehouse (spark)")
    p.add_argument("--pipeline-run", default=None)
    p.add_argument("--fail-on-mismatch", action="store_true")
    args = p.parse_args(argv)
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        p.error(f"unknown steps {unknown}; choose from {', '.join(STEPS)}")
    cfg = json.loads((ROOT / "config" / "lakehouse.json").read_text())
    dates = [date.fromisoformat(x) for x in (args.dates.split(",") if args.dates else cfg["business_dates"])]
    if args.engine == "spark":
        sys.path.insert(0, str(ROOT / "scripts"))
        from run_local import local_spark

        engine = SparkEngine(local_spark(Path(args.warehouse)))
    else:
        engine = ImpalaEngine(cfg["impala"])
    run(engine, args.db_prefix, dates, steps, args.pipeline_run, args.fail_on_mismatch)
    return 0


if __name__ == "__main__":
    sys.exit(main())
