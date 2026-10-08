"""
Drone inspection video: drone/<date>/DRN_<yyyymmdd>_<asset tag>_<n>.avi + .json sidecar.

Four flights a day over pipeline segments, flare stacks and tanks, 20-30 s at 2 fps. The
camera holds each view for 3 s, so a video is 7-10 distinct keyframes. Frames are JPEGs from
the committed library assets/frames/ byte for byte (Motion-JPEG AVI, not MP4: the generator
and the extractor are standard library only), chosen by the asset's planted degradation:
a corroding segment shows mostly severe frames. One flight a day has two unfit frames (the
library's degraded copies). On 2026-10-06 the flight over PL-03-SEG-014 is named and hinted
as PL-03-SEG-041; its GPS track tells the truth.

video_plan(date) is the truth: which library frame is at which second.
"""
from __future__ import annotations

import csv
import json
from datetime import date, datetime, timedelta, timezone

from generators import asset_universe as U
from generators import faults
from generators.formats import mjpeg_avi

FRAMES_DIR = U.ROOT / "assets" / "frames"
HOLD_S = 3
TARGETS = {
    0: ["PL-03-SEG-027", "FL-702", "PL-03-SEG-033", "TK-502"],
    1: ["FL-701", "PL-03-SEG-052", "PL-03-SEG-008", "TK-504"],
    2: ["PL-03-SEG-014", "PL-03-SEG-027", "FL-701", "PL-03-SEG-045"],
    3: ["PL-03-SEG-041", "PL-03-SEG-052", "TK-507", "FL-702"],
    4: ["FL-701", "PL-03-SEG-027", "PL-03-SEG-041", "PL-03-SEG-019"],
    5: ["PL-03-SEG-041", "FL-701", "PL-03-SEG-052", "TK-504"],
}
REFINERY_CENTRE = (23.302, 72.618)


def library() -> list[dict]:
    with open(FRAMES_DIR / "frames.csv") as f:
        return list(csv.DictReader(f))


def _position(a: U.Asset, r) -> tuple[float, float]:
    if a.latitude is not None:
        return a.latitude, a.longitude
    k = sum(map(ord, a.tag))
    return round(REFINERY_CENTRE[0] + (k % 17) * 0.0004, 6), round(REFINERY_CENTRE[1] + (k % 13) * 0.0005, 6)


def video_plan(business_date: date) -> list[dict]:
    r = U.rng("videos", business_date)
    di = (business_date - U.EPOCH.date()).days
    lib = library()
    real = [f for f in lib if not f["degraded"]]
    by_sev = {s: [f for f in real if f["severity"] == s] for s in ("none", "surface", "severe")}
    degraded_of = {f["source_image"]: f for f in lib if f["degraded"]}
    fps = U.CFG["daily"]["video_fps"]
    out = []
    for n, tag in enumerate(TARGETS.get(di, TARGETS[di % 5]), 1):
        a = U.by_tag()[tag]
        named = tag
        if business_date.isoformat() == faults.WRONG_VIDEO_TAG["date"] and tag == faults.WRONG_VIDEO_TAG["true_tag"]:
            named = faults.WRONG_VIDEO_TAG["named_tag"]
        start = datetime.combine(business_date, datetime.min.time(), timezone.utc) + \
            timedelta(minutes=r.randint(7 * 60, 15 * 60))
        s = U.severity_at(tag, U.days_since_epoch(start))
        p_severe = min(0.85, 0.05 + s)
        p_surface = 0.3 if s < 0.8 else 0.12
        seconds = r.randint(*U.CFG["daily"]["video_seconds"]) // HOLD_S * HOLD_S
        keys, used = [], set()
        for k in range(seconds // HOLD_S):
            u = r.random()
            sev = "severe" if u < p_severe else ("surface" if u < p_severe + p_surface else "none")
            choices = [f for f in by_sev[sev] if f["frame"] not in used] or by_sev[sev]
            f = r.choice(choices)
            used.add(f["frame"])
            keys.append(f)
        if n == 3:  # one flight a day: two frames unfit for review (wind gust, low sun)
            for k in r.sample(range(len(keys)), 2):
                keys[k] = degraded_of[keys[k]["source_image"]]
        lat, lon = _position(a, r)
        track = [[round(lat + i * 0.00008, 6), round(lon + i * 0.00009, 6)] for i in range(len(keys))]
        out.append({
            "video_id": f"DRN-{business_date:%Y%m%d}-{n:02d}", "asset_tag": tag, "named_tag": named,
            "file": f"DRN_{business_date:%Y%m%d}_{named}_{n:02d}.avi", "captured_at": start, "fps": fps,
            "duration_s": seconds, "keyframes": keys, "gps_track": track,
            "pilot": r.choice(U.INSPECTORS), "camera": "DJI M300 RTK / Zenmuse H20T",
        })
    return out


def video_bytes(v: dict) -> bytes:
    frames = []
    for f in v["keyframes"]:
        data = (FRAMES_DIR / f["frame"]).read_bytes()
        frames += [data] * (HOLD_S * v["fps"])
    return mjpeg_avi(frames, v["fps"])


def sidecar(v: dict) -> bytes:
    return json.dumps({
        "video_id": v["video_id"], "file": v["file"], "asset_hint": v["named_tag"],
        "captured_at": v["captured_at"].strftime("%Y-%m-%dT%H:%M:%SZ"), "duration_s": v["duration_s"],
        "fps": v["fps"], "camera": v["camera"], "pilot": v["pilot"], "gps_track": v["gps_track"],
    }, indent=1).encode()


def files(business_date: date) -> list[tuple[str, bytes, dict]]:
    out = []
    for v in video_plan(business_date):
        meta = {"video_id": v["video_id"], "frames": v["duration_s"] * v["fps"]}
        out.append((v["file"], video_bytes(v), meta))
        out.append((v["file"][:-4] + ".json", sidecar(v), {"video_id": v["video_id"]}))
    return out
