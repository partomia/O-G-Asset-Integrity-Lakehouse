"""
Well logs: welllogs/<date>/OGX_<well>_RUN<n>.las (LAS 2.0).

12 wells on day 1 (depth 1000-2500 m at 0.5 m: GR, RT, NPHI, RHOB), a re-log of W-07 on
2026-10-07 (run 2). Standard library only (no lasio).
"""
from __future__ import annotations

import math
from datetime import date

from generators import asset_universe as U

RELOGS = {"2026-10-07": [("W-07", 2)]}
CURVES = [("DEPT", "M", "Measured depth"), ("GR", "GAPI", "Gamma ray"), ("RT", "OHMM", "Deep resistivity"),
          ("NPHI", "V/V", "Neutron porosity"), ("RHOB", "G/C3", "Bulk density")]
START, STOP, STEP, NULL = 1000.0, 2500.0, 0.5, -999.25


def las(well: str, run: int, logged: date) -> bytes:
    a = U.by_tag()[well]
    r = U.rng("las", well, run)
    reservoir = [(r.uniform(1300, 2300), r.uniform(15, 40)) for _ in range(3)]
    lines = [
        "~VERSION INFORMATION",
        " VERS.                 2.0 : CWLS LOG ASCII STANDARD - VERSION 2.0",
        " WRAP.                  NO : ONE LINE PER DEPTH STEP",
        "~WELL INFORMATION",
        f" STRT.M          {START:.4f} : START DEPTH",
        f" STOP.M          {STOP:.4f} : STOP DEPTH",
        f" STEP.M          {STEP:.4f} : STEP",
        f" NULL.           {NULL:.2f} : NULL VALUE",
        " COMP.   OGX ENERGY (SYNTHETIC) : COMPANY",
        f" WELL.   OGX {well} : WELL",
        " FLD .   FLD-01 : FIELD",
        f" LOC .   {a.latitude:.6f} {a.longitude:.6f} : LOCATION",
        f" UWI .   OGX-FLD01-{well} : UNIQUE WELL ID",
        f" DATE.   {logged.isoformat()} : LOG DATE",
        f" RUN .   {run} : RUN NUMBER",
        "~CURVE INFORMATION",
    ] + [f" {m:<5}.{u:<5}            : {d}" for m, u, d in CURVES] + [
        "~PARAMETER INFORMATION",
        " BHT .DEGC        92.0 : BOTTOM HOLE TEMPERATURE",
        "~ASCII",
    ]
    d = START
    k = 0
    while d <= STOP + 1e-9:
        inres = any(abs(d - c) < w for c, w in reservoir)
        gr = (35 if inres else 95) + 12 * math.sin(d / 23.0) + r.gauss(0, 4)
        rt = (35 if inres else 4) * (1 + 0.2 * math.sin(d / 11.0)) + abs(r.gauss(0, 0.5))
        nphi = (0.24 if inres else 0.33) + r.gauss(0, 0.01) - (0.01 * (run - 1) if inres else 0)
        rhob = (2.25 if inres else 2.55) + r.gauss(0, 0.02)
        vals = [d, gr, rt, nphi, rhob]
        if k % 997 == 13:
            vals[2] = NULL
        lines.append(" ".join(f"{v:10.4f}" for v in vals))
        d = round(d + STEP, 4)
        k += 1
    return ("\n".join(lines) + "\n").encode("ascii")


def files(business_date: date, first_day: bool) -> list[tuple[str, bytes, dict]]:
    todo = [(a.tag, 1) for a in U.assets() if a.asset_class == "WELL"] if first_day else \
        RELOGS.get(business_date.isoformat(), [])
    rows = int((STOP - START) / STEP) + 1
    return [(f"OGX_{w}_RUN{run}.las", las(w, run, business_date), {"well": w, "run": run, "rows": rows})
            for w, run in todo]
