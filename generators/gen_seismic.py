"""
Seismic surveys: seismic/<date>/OGX_2D_<line>.sgy (SEG-Y rev 1, big-endian, IEEE float).

Two small 2D lines across FLD-01 on day 1 (240 traces x 500 samples at 2 ms each); on
2026-10-05 line L101 is re-sent under a new name (the same bytes: dedup by sha256).

  3200-byte EBCDIC textual header (cp037), 400-byte binary header, then per trace a 240-byte
  trace header and the samples. Standard library only (no segyio).
"""
from __future__ import annotations

import math
import struct
from datetime import date

from generators import asset_universe as U
from generators import faults

SURVEYS = {
    "L101": {"traces": 240, "samples": 500, "interval_us": 2000, "x0": 512000.0, "y0": 2548000.0, "az": 35.0,
             "spacing": 25.0},
    "L102": {"traces": 240, "samples": 500, "interval_us": 2000, "x0": 509500.0, "y0": 2551000.0, "az": 125.0,
             "spacing": 25.0},
}


def textual_header(line: str, s: dict) -> bytes:
    cards = [
        "CLIENT OGX ENERGY (SYNTHETIC)   AREA FLD-01 ONSHORE FIELD 1",
        f"LINE {line}   2D   TRACES {s['traces']}   SAMPLES {s['samples']}   SI {s['interval_us']} US",
        f"FIRST CDP 1001  LAST CDP {1000 + s['traces']}  CDP SPACING {s['spacing']:.1f} M  AZIMUTH {s['az']:.0f}",
        "DATUM WGS84 UTM 43N   COORDINATES IN METRES (SCALAR 1)",
        "PROCESSING: SYNTHETIC REFLECTIVITY CONVOLVED WITH 30 HZ RICKER WAVELET",
        "FORMAT: SEG-Y REV 1, 4-BYTE IEEE FLOAT, BIG-ENDIAN",
    ]
    lines = [f"C{i + 1:2d} {(cards[i] if i < len(cards) else '')}"[:80].ljust(80) for i in range(39)]
    lines.append("C40 END TEXTUAL HEADER".ljust(80))
    return "".join(lines).encode("cp037")


def _ricker(t: float, f: float = 30.0) -> float:
    a = (math.pi * f * t) ** 2
    return (1 - 2 * a) * math.exp(-a)


def segy(line: str) -> bytes:
    s = SURVEYS[line]
    r = U.rng("seismic", line)
    n, ns, dt = s["traces"], s["samples"], s["interval_us"] / 1e6
    out = bytearray(textual_header(line, s))
    binh = bytearray(400)
    struct.pack_into(">iii", binh, 0, 1, 1, 1)                 # job id, line number, reel number
    struct.pack_into(">hh", binh, 12, 1, 0)                    # traces per ensemble, aux traces
    struct.pack_into(">HH", binh, 16, s["interval_us"], s["interval_us"])
    struct.pack_into(">HH", binh, 20, ns, ns)
    struct.pack_into(">h", binh, 24, 5)                        # format code 5 = IEEE float
    struct.pack_into(">h", binh, 26, 1)                        # ensemble fold
    struct.pack_into(">h", binh, 28, 1)                        # sorting: as recorded
    struct.pack_into(">H", binh, 300, 0x0100)                  # SEG-Y revision 1
    struct.pack_into(">h", binh, 302, 1)                       # fixed length traces
    out += binh
    layers = [(0.25 + 0.15 * k + r.uniform(-0.02, 0.02), r.uniform(-1, 1), r.uniform(-0.0004, 0.0004))
              for k in range(5)]
    az = math.radians(s["az"])
    for i in range(n):
        th = bytearray(240)
        x = s["x0"] + i * s["spacing"] * math.sin(az)
        y = s["y0"] + i * s["spacing"] * math.cos(az)
        struct.pack_into(">iiii", th, 0, i + 1, i + 1, 1, i + 1)   # seq in line, seq in file, ffid, channel
        struct.pack_into(">i", th, 20, 1001 + i)                   # CDP
        struct.pack_into(">h", th, 70, 1)                          # coordinate scalar
        struct.pack_into(">iiii", th, 72, int(x), int(y), int(x), int(y))
        struct.pack_into(">HH", th, 114, ns, s["interval_us"])
        struct.pack_into(">ii", th, 180, int(x), int(y))           # CDP X / Y
        samples = []
        for j in range(ns):
            t = j * dt
            v = sum(amp * _ricker(t - (t0 + dip * i)) for t0, amp, dip in layers)
            samples.append(v + r.gauss(0, 0.02))
        out += th + struct.pack(f">{ns}f", *samples)
    return bytes(out)


def files(business_date: date, first_day: bool) -> list[tuple[str, bytes, dict]]:
    out = []
    if first_day:
        for line, s in SURVEYS.items():
            out.append((f"OGX_2D_{line}.sgy", segy(line), {"line": line, "traces": s["traces"]}))
    if business_date.isoformat() == faults.SEISMIC_RESEND["date"]:
        line = faults.SEISMIC_RESEND["line"]
        out.append((f"OGX_2D_{line}_resend.sgy", segy(line), {"line": line, "traces": SURVEYS[line]["traces"]}))
    return out
