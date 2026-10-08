"""
SCADA alarm rules the producer applies, like a DCS: HI / HIHI against config/streaming.json
thresholds (relative to the asset's design pressure for pressure), with hysteresis so an alarm
is raised once and cleared once. Standard library only.
"""
from __future__ import annotations

import json
from pathlib import Path

CFG = json.loads((Path(__file__).resolve().parents[2] / "config" / "streaming.json").read_text())
BAR_TO_PSI = CFG["units"]["bar_to_psi"]


def limits(sensor_tag: str, design_pressure_psi: float | None) -> tuple[float, float] | None:
    """(HI, HIHI) in the sensor's engineering unit, or None when the measure has no alarm."""
    kind = sensor_tag.split("-", 1)[0]
    t = CFG["thresholds"]
    if kind == "PI":
        hi = 0.9 * (design_pressure_psi or t["pressure_psi"]["high"])
        return hi, hi * 1.05
    if kind == "TI":
        return t["temperature_c"]["high"], t["temperature_c"]["high"] * 1.1
    if kind == "VI":
        return 4.5, t["vibration_mm_s"]["high"]
    if kind == "CP":
        return 0.3, t["wall_loss_mm_y"]["high"]
    return None


class AlarmState:
    """Per-tag alarm state; feed readings in event-time order, get alarm events back."""

    def __init__(self):
        self.active: dict[str, str] = {}
        self.n = 0

    def update(self, sensor_tag: str, value: float, unit: str, design_psi: float | None, ts: str) -> list[dict]:
        lim = limits(sensor_tag, design_psi)
        if lim is None:
            return []
        v = value * BAR_TO_PSI if unit == "bar" else value
        hi, hihi = lim
        level = "HIHI" if v >= hihi else ("HI" if v >= hi else None)
        current = self.active.get(sensor_tag)
        out = []
        if level and level != current and (current is None or level == "HIHI"):
            self.n += 1
            self.active[sensor_tag] = level
            out.append({"alarm_id": f"ALM-{self.n:07d}", "sensor_tag": sensor_tag, "event_ts": ts, "alarm_type": level,
                        "priority": "1" if level == "HIHI" else "2", "value": round(v, 4),
                        "limit": round(hihi if level == "HIHI" else hi, 4), "state": "ACTIVE"})
        elif current and v < hi * 0.85:
            self.n += 1
            del self.active[sensor_tag]
            out.append({"alarm_id": f"ALM-{self.n:07d}", "sensor_tag": sensor_tag, "event_ts": ts, "alarm_type": current,
                        "priority": "3", "value": round(v, 4), "limit": round(hi, 4), "state": "CLEARED"})
        return out
