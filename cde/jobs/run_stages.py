"""
Several stages for several business dates in one Spark session (CDE job rsingh-ogx-stages), for
a backfill or a catch-up: one driver start instead of one per stage and date. Dates run in
order and, within a date, stages in the order given, exactly as the DAG would run them.

  cde job run --name rsingh-ogx-stages --arg=--stages --arg=asset,gold,recon \
    --arg=--dates --arg=2026-10-04,2026-10-05 --arg=--db-prefix --arg=rsingh_ogx --arg=--landing --arg=s3a://...
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(JOBS_DIR))

import ogx_common as C  # noqa: E402

STAGES = {"bronze": "ingest_bronze.py", "extract": "extract_unstructured.py", "silver": "build_silver.py",
          "asset": "build_asset_master.py", "gold": "build_gold.py", "recon": "reconcile.py"}


def load_job(filename: str):
    spec = importlib.util.spec_from_file_location(filename[:-3], JOBS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stages", required=True, help=f"comma-separated, from {', '.join(STAGES)}")
    p.add_argument("--dates", required=True, help="comma-separated business dates, run in order")
    p.add_argument("--only", action="append", default=[],
                   help="STAGE=d1,d2: run that stage on these dates only (repeatable)")
    args, rest = p.parse_known_args(argv)
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    unknown = [s for s in stages if s not in STAGES]
    if unknown:
        p.error(f"unknown stages {unknown}")
    only = {k: set(v.split(",")) for k, v in (o.split("=", 1) for o in args.only)}
    spark = C.get_spark("ogx-stages")
    jobs = {s: load_job(STAGES[s]) for s in stages}
    for d in [x.strip() for x in args.dates.split(",") if x.strip()]:
        for s in stages:
            if s in only and d not in only[s]:
                continue
            print(f"=== {s} {d}", flush=True)
            jobs[s].run(spark, ["--business-date", d, *rest])
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
