"""
SEG-Y headers, standard library only: the EBCDIC textual header, the binary header and every
trace header (sequence, CDP, coordinates). Samples are not loaded into tables: the survey stays
whole in raw/ and the lakehouse indexes its geometry.

  parse(data) -> {"textual_header", "line_name", "sample_interval_us", "samples_per_trace",
                  "format_code", "trace_count", "traces": [(seq, cdp, x, y)], "extent": ...}
"""
from __future__ import annotations

import re
import struct

BYTES_PER_SAMPLE = {1: 4, 2: 4, 3: 2, 5: 4, 8: 1}


class SegyError(ValueError):
    pass


def parse(data: bytes) -> dict:
    if len(data) < 3600:
        raise SegyError("shorter than the 3600-byte file header")
    text = data[:3200].decode("cp037", errors="replace")
    if not text.startswith("C"):
        raise SegyError("textual header does not start with a C card")
    interval, _ = struct.unpack(">HH", data[3216:3220])
    ns, _ = struct.unpack(">HH", data[3220:3224])
    fmt = struct.unpack(">h", data[3224:3226])[0]
    if fmt not in BYTES_PER_SAMPLE or ns == 0:
        raise SegyError(f"unsupported format code {fmt} or zero samples")
    tr_len = 240 + ns * BYTES_PER_SAMPLE[fmt]
    body = len(data) - 3600
    if body % tr_len:
        raise SegyError(f"{body} trace bytes is not a whole number of {tr_len}-byte traces")
    n = body // tr_len
    traces = []
    for i in range(n):
        off = 3600 + i * tr_len
        seq = struct.unpack(">i", data[off:off + 4])[0]
        cdp = struct.unpack(">i", data[off + 20:off + 24])[0]
        scalar = struct.unpack(">h", data[off + 70:off + 72])[0] or 1
        x, y = struct.unpack(">ii", data[off + 180:off + 188])
        f = (1.0 / -scalar) if scalar < 0 else float(scalar)
        traces.append((seq, cdp, x * f, y * f))
    cards = [text[i:i + 80].rstrip() for i in range(0, 3200, 80)]
    m = re.search(r"LINE\s+(\w+)", text)
    xs, ys = [t[2] for t in traces], [t[3] for t in traces]
    return {
        "textual_header": "\n".join(c for c in cards if c.strip()),
        "line_name": m.group(1) if m else None,
        "sample_interval_us": interval, "samples_per_trace": ns, "format_code": fmt,
        "trace_count": n, "record_length_ms": ns * interval / 1000.0, "traces": traces,
        "extent": {"min_x": min(xs), "max_x": max(xs), "min_y": min(ys), "max_y": max(ys),
                   "first_cdp": traces[0][1], "last_cdp": traces[-1][1]} if traces else {},
    }
