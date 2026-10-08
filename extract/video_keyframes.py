"""
Keyframes of a Motion-JPEG AVI, standard library only: walk the RIFF tree, read the main
header (fps, frame count, size), and take a frame as a keyframe whenever its JPEG bytes change
(the drone holds each view for a few seconds). Keyframes are kept verbatim, so each one's
sha256 identifies the exact image; CAI scoring finds it in assets/frames/ by that hash.

Blur and exposure are scored in CAI (frame_qc head) where an image decoder is available; here
`bytes_per_kpx` (JPEG bytes per 1,000 pixels) is a cheap sharpness proxy: blurred frames
compress smaller.

  parse(data) -> {"fps", "frame_count", "width", "height", "duration_s",
                  "keyframes": [{"frame_index", "t_seconds", "sha256", "size", "bytes_per_kpx", "jpeg"}]}
"""
from __future__ import annotations

import hashlib
import struct


class AviError(ValueError):
    pass


def _walk(data: bytes, start: int, end: int):
    i = start
    while i + 8 <= end:
        fourcc = data[i:i + 4]
        size = struct.unpack("<I", data[i + 4:i + 8])[0]
        body = i + 8
        if fourcc in (b"LIST", b"RIFF"):
            yield fourcc + data[body:body + 4], body + 4, body + size
            yield from _walk(data, body + 4, min(body + size, end))
        else:
            yield fourcc, body, body + size
        i = body + size + (size & 1)


def parse(data: bytes) -> dict:
    if data[:4] != b"RIFF" or data[8:12] != b"AVI ":
        raise AviError("not a RIFF AVI file")
    fps = width = height = total = None
    frames = []
    for fourcc, s, e in _walk(data, 12, len(data)):
        if fourcc == b"avih":
            us, _, _, _, total, _, _, _, width, height = struct.unpack("<10I", data[s:s + 40])
            fps = round(1_000_000 / us) if us else None
        elif fourcc[2:4] in (b"dc", b"db") and fourcc[:2].isdigit():
            frames.append(data[s:e])
    if not frames:
        raise AviError("no video frames")
    if not frames[0].startswith(b"\xff\xd8"):
        raise AviError("frames are not JPEG (expected Motion-JPEG)")
    fps = fps or 1
    keys, prev = [], None
    for i, f in enumerate(frames):
        h = hashlib.sha256(f).hexdigest()
        if h != prev:
            keys.append({"frame_index": i, "t_seconds": round(i / fps, 3), "sha256": h, "size": len(f),
                         "bytes_per_kpx": round(len(f) / max(1, (width or 1) * (height or 1)) * 1000, 2),
                         "jpeg": f})
            prev = h
    return {"fps": fps, "frame_count": len(frames), "header_frames": total, "width": width, "height": height,
            "duration_s": round(len(frames) / fps, 2), "keyframes": keys}
