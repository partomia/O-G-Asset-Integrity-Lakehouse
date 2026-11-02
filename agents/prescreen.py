"""
Anomaly pre-screen before any LLM call (the AMP's rolling z-score, here on the 15-minute window
means of silver.sensor_window), plus the flags the streaming pipeline already sets: stuck
(the asset is abstained from IRE), breach of the high threshold, bar readings (unit change).
A quick analysis with nothing flagged short-circuits: no tokens spent.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable


@dataclass
class Signal:
    asset_tag: str
    sensor_tag: str
    measurement: str
    kind: str                 # zscore | breach | stuck | unit_change
    window_start: str
    value: float | None = None
    zscore: float | None = None
    baseline: float | None = None
    count: int = 1

    def line(self) -> str:
        if self.kind == "zscore":
            return (f"{self.asset_tag} {self.sensor_tag} {self.measurement}={self.value:.2f} "
                    f"(z={self.zscore:+.1f} vs 6 h mean {self.baseline:.2f}) at {self.window_start}")
        what = {"breach": "high-threshold breach windows", "stuck": "stuck windows (flat line)",
                "unit_change": "readings in bar instead of psi"}[self.kind]
        tail = f", max {self.value:.2f}" if self.value is not None else ""
        return f"{self.asset_tag} {self.sensor_tag} {self.measurement}: {self.count} {what}{tail}, from {self.window_start}"


@dataclass
class Prescreen:
    signals: list[Signal] = field(default_factory=list)
    windows_scanned: int = 0

    @property
    def flagged_assets(self) -> list[str]:
        seen: dict[str, None] = {}
        for s in self.signals:
            seen.setdefault(s.asset_tag)
        return list(seen)

    @property
    def has_signals(self) -> bool:
        return bool(self.signals)

    def by_kind(self) -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for s in self.signals:
            out[s.kind] += 1
        return dict(out)

    def to_context_block(self, top: int = 15) -> str:
        if not self.signals:
            return f"No anomalies in {self.windows_scanned} windows: no z-score outliers, breaches, stuck sensors or unit changes."
        kinds = ", ".join(f"{k} {v}" for k, v in sorted(self.by_kind().items()))
        lines = [f"{len(self.signals)} signals on {len(self.flagged_assets)} assets in {self.windows_scanned} windows ({kinds}):"]
        lines += [f"  - {s.line()}" for s in self.signals[:top]]
        if len(self.signals) > top:
            lines.append(f"  - ... {len(self.signals) - top} more")
        return "\n".join(lines)


def _f(v) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else math.nan
    except (TypeError, ValueError):
        return math.nan


DAILY_FLAGS = (("stuck", "stuck_windows", "is_stuck"), ("breach", "breach_windows", "is_breach"),
               ("unit_change", "bar_readings", "n_bar"))


def scan(windows: Iterable[dict], rolling: int = 24, threshold: float = 3.0, min_rel_change: float = 0.02,
         daily: list[dict] | None = None) -> Prescreen:
    """`windows`: rows of silver.sensor_window with asset_tag added (one facility, one day).
    `daily`: gold.fact_sensor_daily rows with `tag`. When given, stuck / breach / unit-change counts
    come from it (the certified layer behind IRE), the windows only time them; silver windows of an
    older run can carry a threshold the gold layer no longer uses."""
    series: dict[tuple[str, str], list[dict]] = defaultdict(list)
    n = 0
    for w in windows:
        series[(w["sensor_tag"], w["measurement"])].append(w)
        n += 1
    zs: list[Signal] = []
    flags: list[Signal] = []
    for rows in series.values():
        rows.sort(key=lambda r: str(r["window_start"]))
    if daily is not None:
        for d in daily:
            rows = series.get((d["sensor_tag"], d["measurement"]), [])
            for kind, col, wcol in DAILY_FLAGS:
                cnt = int(d.get(col) or 0)
                if cnt:
                    first = next((str(r["window_start"]) for r in rows if r.get(wcol)), "the business date")
                    mx = _f(d.get("max_value"))
                    flags.append(Signal(d.get("tag") or d["sensor_tag"], d["sensor_tag"], d["measurement"], kind, first,
                                        value=None if math.isnan(mx) or kind == "stuck" else mx, count=cnt))
    for (sensor, meas), rows in series.items():
        asset = rows[0].get("asset_tag") or sensor
        if daily is None:
            for kind, _, col in DAILY_FLAGS:
                hit = [r for r in rows if (r.get(col) or 0)]
                if hit:
                    mx = max((_f(r.get("max_value")) for r in hit), default=math.nan)
                    flags.append(Signal(asset, sensor, meas, kind, str(hit[0]["window_start"]),
                                        value=None if math.isnan(mx) or kind == "stuck" else mx, count=len(hit)))
        vals = [_f(r.get("mean_value")) for r in rows]
        for i in range(rolling, len(vals)):
            if math.isnan(vals[i]) or rows[i].get("is_stuck") or rows[i].get("n_bar"):
                continue
            ref = [v for v, r in zip(vals[i - rolling:i], rows[i - rolling:i])
                   if not math.isnan(v) and not r.get("n_bar")]
            if len(ref) < rolling // 2:
                continue
            mu = sum(ref) / len(ref)
            sd = math.sqrt(sum((v - mu) ** 2 for v in ref) / len(ref))
            if sd < 1e-6 or abs(mu) > 0 and sd / abs(mu) < 1e-3:
                continue
            z = (vals[i] - mu) / sd
            if abs(z) >= threshold and abs(vals[i] - mu) >= min_rel_change * max(abs(mu), 1e-9):
                zs.append(Signal(asset, sensor, meas, "zscore", str(rows[i]["window_start"]), value=vals[i],
                                 zscore=z, baseline=mu))
    # one z-score signal per sensor (its largest), so a slow ramp does not flood the prompt
    best: dict[tuple[str, str], Signal] = {}
    for s in zs:
        k = (s.sensor_tag, s.measurement)
        if k not in best or abs(s.zscore) > abs(best[k].zscore):
            best[k] = s
    order = {"stuck": 0, "unit_change": 1, "breach": 2}
    flags.sort(key=lambda s: (order[s.kind], -s.count))
    return Prescreen(flags + sorted(best.values(), key=lambda s: -abs(s.zscore)), n)
