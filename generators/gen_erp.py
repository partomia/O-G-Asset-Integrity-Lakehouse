"""
EAM / SAP PM style structured sources:

  erp/<date>/asset_register_<yyyymmdd>.sql   day 1: mysqldump of table `equi` (every asset)
  erp/<date>/asset_cdc_<yyyymmdd>.jsonl      later days: CDC events (U / I / D) on `equi`
  erp/<date>/work_orders_<yyyymmdd>.psv      daily: pipe-delimited work orders, trailer T|count|total

Dates in the dump mix ISO and SAP's DD.MM.YYYY (as exported by different plants), so the
contract lists both formats. Standard library only.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from generators import asset_universe as U
from generators import faults

EQUI_COLUMNS = ["equnr", "tplnr", "eqktx", "eqart", "swerk", "abckz", "werks_mat", "design_press_psi",
                "inbdt", "status", "gps_lat", "gps_lon", "chain_from_km", "chain_to_km", "insp_interval_d",
                "last_insp_date"]
WO_COLUMNS = ["aufnr", "equnr", "tplnr", "auart", "priority", "status", "created_at", "closed_at", "cost_usd",
              "short_text"]
PLANT = {f["facility_id"]: f["plant"] for f in U.FACILITIES}


def _q(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sap_date(iso: str, plant: str) -> str:
    """Field and pipeline plants export DD.MM.YYYY, the refinery ISO."""
    if plant in ("FLD1", "PL03"):
        y, m, d = iso.split("-")
        return f"{d}.{m}.{y}"
    return iso


def equi_row(a: U.Asset) -> list:
    plant = PLANT[a.facility_id]
    return [a.equnr, a.func_loc, a.description, a.asset_class, plant, a.criticality, a.material,
            a.design_pressure_psi, _sap_date(a.install_date, plant), "INST", a.latitude, a.longitude,
            a.chainage_from_km, a.chainage_to_km, a.inspection_interval_days, _sap_date(a.last_inspection, plant)]


def asset_register_dump(business_date: date) -> bytes:
    rows = [equi_row(a) for a in U.assets()]
    out = [
        "-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)",
        f"-- Host: eam-prod    Database: eam    Dump date: {business_date.isoformat()} 01:00:00",
        "-- ------------------------------------------------------",
        "DROP TABLE IF EXISTS `equi`;",
        "CREATE TABLE `equi` (",
        "  `equnr` varchar(18) NOT NULL,", "  `tplnr` varchar(40) DEFAULT NULL,",
        "  `eqktx` varchar(80) DEFAULT NULL,", "  `eqart` varchar(20) DEFAULT NULL,",
        "  `swerk` varchar(4) DEFAULT NULL,", "  `abckz` char(1) DEFAULT NULL,",
        "  `werks_mat` varchar(40) DEFAULT NULL,", "  `design_press_psi` decimal(9,1) DEFAULT NULL,",
        "  `inbdt` varchar(10) DEFAULT NULL,", "  `status` varchar(6) DEFAULT NULL,",
        "  `gps_lat` decimal(9,6) DEFAULT NULL,", "  `gps_lon` decimal(9,6) DEFAULT NULL,",
        "  `chain_from_km` decimal(7,3) DEFAULT NULL,", "  `chain_to_km` decimal(7,3) DEFAULT NULL,",
        "  `insp_interval_d` int DEFAULT NULL,", "  `last_insp_date` varchar(10) DEFAULT NULL,",
        "  PRIMARY KEY (`equnr`)",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
        "LOCK TABLES `equi` WRITE;",
    ]
    for i in range(0, len(rows), 50):
        out.append("INSERT INTO `equi` VALUES " + ",".join("(" + ",".join(_q(v) for v in r) + ")"
                                                         for r in rows[i:i + 50]) + ";")
    out += ["UNLOCK TABLES;", f"-- Dump completed on {business_date.isoformat()} 01:00:04", ""]
    return "\n".join(out).encode("utf-8")


def cdc_events(business_date: date) -> list[dict]:
    """Changes made in EAM during the previous day (a few criticality, inspection-date and status
    updates, plus the planted insert and decommission)."""
    r = U.rng("cdc", business_date)
    ts0 = datetime.combine(business_date - timedelta(days=1), datetime.min.time(), timezone.utc)
    ev = []
    pool = [a for a in U.assets() if a.facility_id == "REF-U1"]
    n = U.CFG["daily"]["cdc_changes"]
    for i in range(n):
        a = r.choice(pool)
        t = ts0 + timedelta(minutes=r.randint(420, 1080))
        kind = r.choice(["insp", "insp", "crit", "material_note"])
        if kind == "insp":
            fields = {"last_insp_date": (business_date - timedelta(days=1)).isoformat()}
        elif kind == "crit":
            fields = {"abckz": "A" if a.criticality != "A" else "B"}
        else:
            fields = {"werks_mat": a.material + " (verified)"}
        ev.append({"op": "U", "equnr": a.equnr, "changed_at": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "changed_by": "EAM_BATCH", "fields": fields})
    if business_date.isoformat() == faults.NEW_ASSET["date"]:
        ref = U.by_tag()["PSV-867"]
        row = dict(zip(EQUI_COLUMNS, equi_row(ref)))
        row.update({"equnr": faults.NEW_ASSET["equnr"], "tplnr": "REF-U1-PSV-868",
                    "eqktx": "Pressure safety valve PSV-868", "inbdt": (business_date - timedelta(days=1)).isoformat(),
                    "last_insp_date": (business_date - timedelta(days=1)).isoformat()})
        ev.append({"op": "I", "equnr": row["equnr"], "changed_at": (ts0 + timedelta(hours=15)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"), "changed_by": "J.PEREIRA", "fields": row})
    if business_date.isoformat() == faults.DECOMMISSIONED["date"]:
        a = U.by_tag()[faults.DECOMMISSIONED["tag"]]
        ev.append({"op": "U", "equnr": a.equnr, "changed_at": (ts0 + timedelta(hours=16)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"), "changed_by": "J.PEREIRA", "fields": {"status": "DECOM"}})
    return sorted(ev, key=lambda e: e["changed_at"])


def asset_cdc(business_date: date) -> bytes:
    return ("\n".join(json.dumps(e, separators=(",", ":")) for e in cdc_events(business_date)) + "\n").encode()


def work_orders(business_date: date) -> tuple[list[list], float]:
    r = U.rng("wo", business_date)
    xs = list(U.assets())
    rows, total = [], 0.0
    day = (datetime.combine(business_date, datetime.min.time(), timezone.utc) - U.EPOCH).days
    n = U.CFG["daily"]["work_orders"]
    for i in range(n):
        degrading = [U.by_tag()[t] for t in U.DEGRADATION if U.severity_at(t, day) > 0.3]
        a = r.choice(degrading) if degrading and r.random() < 0.25 else r.choice(xs)
        corrective = a.tag in U.DEGRADATION and U.severity_at(a.tag, day) > 0.3
        auart = "PM01" if corrective or r.random() < 0.2 else r.choice(["PM02", "PM02", "PM03"])
        created = datetime.combine(business_date, datetime.min.time()) + timedelta(minutes=r.randint(360, 1020))
        closed = created + timedelta(hours=r.randint(2, 30)) if r.random() < 0.55 else None
        cost = round(r.uniform(400, 9000) * (3 if auart == "PM01" else 1), 2)
        total += cost
        text = {"PM01": f"Corrective: {r.choice(['high vibration', 'leak at flange', 'wall thinning', 'seal failure', 'hot spot'])} on {a.tag}",
                "PM02": f"Preventive maintenance {a.tag}", "PM03": f"Inspection follow-up {a.tag}"}[auart]
        rows.append([f"4{business_date:%y%m%d}{i + 1:03d}", a.equnr, a.func_loc, auart,
                     "1" if auart == "PM01" and a.criticality == "A" else r.choice("234"),
                     "TECO" if closed else r.choice(["CRTD", "REL"]), created.strftime("%Y-%m-%d %H:%M:%S"),
                     closed.strftime("%Y-%m-%d %H:%M:%S") if closed else "", f"{cost:.2f}", text])
    return rows, round(total, 2)


def work_orders_psv(business_date: date) -> bytes:
    rows, total = work_orders(business_date)
    count = len(rows) + (1 if business_date.isoformat() == faults.WO_TRAILER_OFF_DATE else 0)
    lines = ["|".join(WO_COLUMNS)] + ["|".join(str(v) for v in r) for r in rows] + [f"T|{count}|{total:.2f}"]
    return ("\n".join(lines) + "\n").encode()


def files(business_date: date, first_day: bool) -> list[tuple[str, bytes, dict]]:
    """(file name, bytes, manifest extras) for the erp source on one date."""
    d = f"{business_date:%Y%m%d}"
    out = []
    if first_day:
        out.append((f"asset_register_{d}.sql", asset_register_dump(business_date),
                    {"entity": "erp_asset", "records": len(U.assets())}))
    else:
        out.append((f"asset_cdc_{d}.jsonl", asset_cdc(business_date),
                    {"entity": "erp_asset_cdc", "records": len(cdc_events(business_date))}))
    rows, total = work_orders(business_date)
    out.append((f"work_orders_{d}.psv", work_orders_psv(business_date),
                {"entity": "erp_work_order", "records": len(rows), "control_total": total}))
    return out
