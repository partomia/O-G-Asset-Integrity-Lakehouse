"""
The one seeded asset universe every source is generated from, so every PDF, video frame,
drawing tag, well log and sensor reading points at an asset that exists in the master data.

  FLD-01   onshore field, 12 wells                      W-01 .. W-12
  PL-03    40 km gathering pipeline in 60 segments      PL-03-SEG-001 .. PL-03-SEG-060
  REF-U1   refinery unit, 150 tagged equipment items    P-101A, K-201, V-301, E-401, ...

222 assets, 200 sensor tags (ISA style: PI-101A.PV is the pressure transmitter on P-101A),
and a handful of assets with planted degradation: the truth the stream, the inspection
reports, the drone frames and the equipment_risk labels all follow.

Standard library only (runs on the CDE driver and in CAI alike).
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import zlib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "lakehouse.json").read_text())
SEED = CFG["seed"]
EPOCH = datetime(2026, 10, 4, tzinfo=timezone.utc)      # first business date, 00:00 UTC

FACILITIES = [
    {"facility_id": "FLD-01", "name": "Onshore Field 1", "facility_type": "FIELD", "plant": "FLD1"},
    {"facility_id": "PL-03", "name": "Gathering Pipeline 3 (40 km)", "facility_type": "PIPELINE", "plant": "PL03"},
    {"facility_id": "REF-U1", "name": "Refinery Unit 1 (crude distillation)", "facility_type": "REFINERY_UNIT",
     "plant": "REF1"},
]

# class code -> (asset class, description stem, count, material, design psi range, interval days, base criticality)
EQUIPMENT = [
    ("P", "PUMP", "Centrifugal pump", 40, "CS A216 WCB", (300, 740), 365, "B"),
    ("K", "COMPRESSOR", "Gas compressor", 6, "CS A216 WCB", (900, 1480), 365, "A"),
    ("V", "VESSEL", "Pressure vessel", 24, "CS SA-516-70", (150, 600), 730, "A"),
    ("E", "HEAT_EXCHANGER", "Shell and tube exchanger", 30, "CS SA-179", (150, 450), 730, "B"),
    ("TK", "TANK", "Atmospheric storage tank", 10, "CS A283-C", (15, 15), 1825, "B"),
    ("C", "COLUMN", "Distillation column", 4, "CS SA-516-70 clad", (50, 150), 1460, "A"),
    ("FL", "FLARE_STACK", "Flare stack", 2, "CS A106-B", (50, 50), 1095, "A"),
    ("XV", "VALVE", "Shutdown valve", 17, "CS A216 WCB", (300, 740), 730, "B"),
    ("PSV", "RELIEF_VALVE", "Pressure safety valve", 17, "SS 316", (300, 740), 365, "A"),
]
CLASS_NUMBER_BASE = {"P": 101, "K": 201, "V": 301, "E": 401, "TK": 501, "C": 601, "FL": 701, "XV": 801, "PSV": 851}

UNITS = {"PI": "psi", "TI": "degC", "VI": "mm/s", "CP": "mm/y"}
MEASURE = {"PI": "pressure", "TI": "temperature", "VI": "vibration", "CP": "corrosion_rate"}

INSPECTORS = ["A. Raman", "B. Okafor", "C. Lindqvist", "D. Mehta", "E. Haddad", "F. Novak", "G. Silva",
              "H. Tanaka"]

# Planted degradation: the truth. start/end are days after EPOCH (fractions allowed); severity
# ramps 0 -> 1 between them; an integrity event (the equipment_risk label) happens at `end`.
DEGRADATION = {
    "P-105A": {"kind": "vibration", "start": -6.0, "end": 9.0},
    "P-117B": {"kind": "vibration", "start": 1.5, "end": 14.0},
    "K-203": {"kind": "vibration", "start": -2.0, "end": 12.0},
    "E-412": {"kind": "fouling", "start": -10.0, "end": 20.0},
    "V-307": {"kind": "wall_loss", "start": -60.0, "end": 8.0},
    "TK-504": {"kind": "wall_loss", "start": -90.0, "end": 25.0},
    "FL-701": {"kind": "corrosion", "start": -45.0, "end": 6.0},
    "PL-03-SEG-027": {"kind": "corrosion", "start": -40.0, "end": 5.0},
    "PL-03-SEG-041": {"kind": "corrosion", "start": -20.0, "end": 18.0},
    "PL-03-SEG-052": {"kind": "corrosion", "start": -30.0, "end": 28.0},
}


@dataclass
class Asset:
    equnr: str                       # EAM equipment number (the ERP key)
    func_loc: str                    # EAM functional location, e.g. REF-U1-P-101A
    tag: str                         # P&ID tag / segment id / well id, e.g. P-101A
    asset_class: str
    facility_id: str
    description: str
    criticality: str
    material: str
    design_pressure_psi: float
    install_date: str
    inspection_interval_days: int
    last_inspection: str
    latitude: float | None = None
    longitude: float | None = None
    chainage_from_km: float | None = None
    chainage_to_km: float | None = None
    sensor_tags: list[str] = field(default_factory=list)

    @property
    def asset_id(self) -> str:
        """The canonical id the asset master assigns (survived from the EAM record)."""
        return f"OGX-{self.equnr}"

    def due_date(self) -> date:
        return date.fromisoformat(self.last_inspection) + timedelta(days=self.inspection_interval_days)


def rng(*key) -> random.Random:
    h = hashlib.sha256(json.dumps([SEED, *key], default=str).encode()).hexdigest()
    return random.Random(int(h[:16], 16))


def _pipeline_point(km: float) -> tuple[float, float]:
    """A gentle 40 km arc north-east from the field (synthetic coordinates)."""
    lat0, lon0 = 23.050, 72.350
    return (round(lat0 + km * 0.0062 + 0.01 * math.sin(km / 7.0), 6),
            round(lon0 + km * 0.0071 + 0.008 * math.cos(km / 9.0), 6))


@lru_cache(maxsize=1)
def assets() -> tuple[Asset, ...]:
    out: list[Asset] = []
    n = 0

    def equnr() -> str:
        nonlocal n
        n += 1
        return f"1000{n:04d}"

    r = rng("universe")
    for i in range(1, CFG["universe"]["wells"] + 1):
        tag = f"W-{i:02d}"
        lat, lon = 23.050 - 0.012 * (i % 4) - r.uniform(0, 0.01), 72.350 - 0.015 * (i // 4) - r.uniform(0, 0.01)
        out.append(Asset(equnr(), f"FLD-01-{tag}", tag, "WELL", "FLD-01", f"Producing well {tag}", "A",
                         "API 5CT L80", 3000.0, f"{2014 + i % 8}-0{1 + i % 9}-15", 365,
                         (EPOCH.date() - timedelta(days=r.randint(30, 340))).isoformat(),
                         latitude=round(lat, 6), longitude=round(lon, 6)))
    segs = CFG["universe"]["pipeline_segments"]
    seg_km = CFG["universe"]["pipeline_km"] / segs
    for i in range(1, segs + 1):
        tag = f"PL-03-SEG-{i:03d}"
        a, b = round((i - 1) * seg_km, 3), round(i * seg_km, 3)
        lat, lon = _pipeline_point((a + b) / 2)
        crit = "A" if i in (1, 27, 41, 60) or i % 10 == 0 else "B"
        out.append(Asset(equnr(), tag, tag, "PIPE_SEGMENT", "PL-03", f"Pipeline 3 segment {i} (km {a:.1f}-{b:.1f})",
                         crit, "API 5L X52", 1440.0, "2011-06-01", 1095,
                         (EPOCH.date() - timedelta(days=r.randint(60, 1000))).isoformat(),
                         latitude=lat, longitude=lon, chainage_from_km=a, chainage_to_km=b))
    for code, cls, desc, count, material, (plo, phi), interval, crit in EQUIPMENT:
        base = CLASS_NUMBER_BASE[code]
        for k in range(count):
            if code == "P":
                num = f"{base + k // 2}{'AB'[k % 2]}"
            else:
                num = str(base + k)
            tag = f"{code}-{num}"
            c = crit if r.random() > 0.25 else ("A" if crit == "B" else "B")
            if r.random() < 0.15:
                c = "C"
            out.append(Asset(equnr(), f"REF-U1-{tag}", tag, cls, "REF-U1", f"{desc} {tag}", c, material,
                             float(r.randint(plo // 10, phi // 10) * 10) if phi > plo else float(plo),
                             f"{2009 + r.randint(0, 12)}-{r.randint(1, 12):02d}-{r.randint(1, 28):02d}", interval,
                             (EPOCH.date() - timedelta(days=r.randint(20, interval + 120))).isoformat()))
    _assign_sensors(out)
    return tuple(out)


def _assign_sensors(items: list[Asset]) -> None:
    by_tag = {a.tag: a for a in items}
    for a in items:
        num = a.tag.split("-", 1)[1] if a.facility_id == "REF-U1" else None
        if a.asset_class == "PUMP":
            a.sensor_tags = [f"VI-{num}.PV"] + ([f"PI-{num}.PV"] if num.endswith("A") else [])
        elif a.asset_class == "COMPRESSOR":
            a.sensor_tags = [f"VI-{num}.PV", f"PI-{num}.PV", f"TI-{num}.PV"]
        elif a.asset_class in ("VESSEL", "COLUMN"):
            a.sensor_tags = [f"PI-{num}.PV", f"TI-{num}.PV"]
        elif a.asset_class == "HEAT_EXCHANGER":
            a.sensor_tags = [f"TI-{num}.PV"]
        elif a.asset_class == "PIPE_SEGMENT":
            i = int(a.tag[-3:])
            a.sensor_tags = ([f"CP-PL03-{i:03d}.PV"] if i % 3 == 0 or a.tag in DEGRADATION else []) + \
                            ([f"PI-PL03-{i:03d}.PV"] if i in (1, 20, 40, 60) else [])
        elif a.asset_class == "WELL":
            a.sensor_tags = [f"PI-W{a.tag[-2:]}.PV"]
    n = sum(len(a.sensor_tags) for a in items)
    target = CFG["universe"]["sensor_tags"]
    # trim pipeline probes from the end until the count matches the configured 200
    for a in reversed(items):
        if n <= target:
            break
        if a.asset_class == "PIPE_SEGMENT" and a.tag not in DEGRADATION and len(a.sensor_tags) == 1 \
                and a.sensor_tags[0].startswith("CP-"):
            a.sensor_tags = []
            n -= 1
    assert n == target or n < target, n
    del by_tag


def by_tag() -> dict[str, Asset]:
    return {a.tag: a for a in assets()}


def by_equnr() -> dict[str, Asset]:
    return {a.equnr: a for a in assets()}


def sensor_index() -> dict[str, Asset]:
    """sensor tag -> asset"""
    return {s: a for a in assets() for s in a.sensor_tags}


def sensors() -> list[dict]:
    out = []
    for a in assets():
        for s in a.sensor_tags:
            kind = s.split("-", 1)[0]
            out.append({"sensor_tag": s, "asset_tag": a.tag, "measure": MEASURE[kind], "unit": UNITS[kind]})
    return out


# ---------------------------------------------------------------- degradation truth


def days_since_epoch(ts: datetime) -> float:
    return (ts - EPOCH).total_seconds() / 86400.0


def severity_at(tag: str, day: float) -> float:
    """0 (healthy) .. 1 (failed) for a planted degradation; 0 for every other asset."""
    d = DEGRADATION.get(tag)
    if not d:
        return 0.0
    return max(0.0, min(1.0, (day - d["start"]) / (d["end"] - d["start"])))


def event_within(tag: str, day: float, horizon_days: float = 30.0) -> bool:
    d = DEGRADATION.get(tag)
    return bool(d) and day <= d["end"] <= day + horizon_days


BASE = {  # (measure kind, asset class) -> (base value, noise sd)
    ("PI", "PUMP"): (450.0, 6.0), ("PI", "COMPRESSOR"): (920.0, 9.0), ("PI", "VESSEL"): (160.0, 2.5),
    ("PI", "COLUMN"): (32.0, 0.6), ("PI", "PIPE_SEGMENT"): (1080.0, 8.0), ("PI", "WELL"): (1350.0, 12.0),
    ("TI", "COMPRESSOR"): (95.0, 0.8), ("TI", "VESSEL"): (120.0, 0.8), ("TI", "COLUMN"): (142.0, 0.9),
    ("TI", "HEAT_EXCHANGER"): (110.0, 0.9), ("VI", "PUMP"): (2.1, 0.15), ("VI", "COMPRESSOR"): (2.8, 0.2),
    ("CP", "PIPE_SEGMENT"): (0.08, 0.01),
}
DELTA = {"vibration": {"VI": 6.5, "TI": 12.0}, "fouling": {"TI": 38.0}, "corrosion": {"CP": 0.9},
         "wall_loss": {"PI": -12.0}}


def true_value(sensor_tag: str, ts: datetime, noise: random.Random | None = None) -> float:
    """The engineering value of a sensor at an instant: base + daily cycle + degradation + noise."""
    a = sensor_index()[sensor_tag]
    kind = sensor_tag.split("-", 1)[0]
    base, sd = BASE.get((kind, a.asset_class), (100.0, 1.0))
    if kind == "PI" and a.design_pressure_psi:
        # operating pressure ~60 % of design, so HI (90 % of design) means something
        base, sd = 0.6 * a.design_pressure_psi, 0.006 * a.design_pressure_psi
    day = days_since_epoch(ts)
    v = base * (1 + 0.01 * math.sin(2 * math.pi * (day % 1.0) + zlib.crc32(sensor_tag.encode()) % 7))
    deg = DEGRADATION.get(a.tag)
    if deg:
        v += DELTA.get(deg["kind"], {}).get(kind, 0.0) * severity_at(a.tag, day) ** 1.5
    if noise is not None:
        v += noise.gauss(0, sd)
    return round(v, 4)


def wall_loss_pct(tag: str, day: float, r: random.Random) -> float:
    """Measured wall loss of an inspection (UT): small for healthy assets, rising with planted
    wall_loss / corrosion degradation."""
    a = by_tag()[tag]
    base = r.uniform(1.0, 7.5) if a.asset_class not in ("PIPE_SEGMENT", "TANK", "VESSEL") else r.uniform(2.0, 9.0)
    deg = DEGRADATION.get(tag)
    if deg and deg["kind"] in ("wall_loss", "corrosion"):
        base += 32.0 * severity_at(tag, day)
    return round(min(base, 62.0), 1)


def asdicts() -> list[dict]:
    return [asdict(a) | {"asset_id": a.asset_id} for a in assets()]


if __name__ == "__main__":
    xs = assets()
    from collections import Counter

    print(len(xs), "assets", Counter(a.facility_id for a in xs), sum(len(a.sensor_tags) for a in xs), "sensor tags")
