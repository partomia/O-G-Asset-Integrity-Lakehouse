"""
Job ogx-stream-producer: the field's sensor telemetry and SCADA alarms onto Kafka.

One reading per sensor tag (200 tags) per interval, from the seeded asset universe: base value,
daily cycle, noise and the planted degradation (a pump's vibration drifts up, a corrosion probe
climbs), so the stream carries real signal. A DCS-style alarm rule raises HI / HIHI alarms on
ogx.scada.alarm. Every event-time minute gets a control message on ogx.control with the count
sent per tag: the stream reconciliation's control total.

Planted stream faults (config/streaming.json producer.*):
  late           ~1 % of readings sent 2-8 minutes late (inside the 10-minute watermark)
  very late      ~0.1 % sent 15-30 minutes late (beyond the watermark: recon reports LATE)
  out of order   ~2 % swapped with the next reading of the minute
  stuck sensor   VI-112B.PV flat-lines from 2026-10-05 12:00 UTC
  unit change    PI-105A.PV switches from psi to bar at 2026-10-06 10:00 UTC

Modes:
  backfill  the business dates at --interval (default 30 s), as fast as the sink takes them
  live      real time at 5 s per tag (~40 msg/s) for --duration seconds (0 = until stopped)

Sinks: kafka (OGX_KAFKA_*; SASL_SSL on the cluster) or files (JSON lines per topic, the CI and
laptop path the streaming jobs read with a file source).

  python -m stream.producer.sensor_producer --sink files --out data/stream --mode backfill
  python stream/producer/sensor_producer.py --sink kafka --mode live --duration 3600
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:  # CAI job kernels run the script without __file__; cwd is the project
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from generators import asset_universe as U  # noqa: E402
from stream.producer.alarm_rules import AlarmState  # noqa: E402

CFG = json.loads((ROOT / "config" / "streaming.json").read_text())
P = CFG["producer"]
TOPIC = {k: v["name"] for k, v in CFG["topics"].items()}
STUCK_FROM = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
UNIT_CHANGE_FROM = datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc)
BAR_TO_PSI = CFG["units"]["bar_to_psi"]
MAX_DELAY_S = 30 * 60


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


class Generator:
    """Readings, alarms and control counts in the order a field gateway would send them."""

    def __init__(self, run_id: str, seed: int = U.SEED, allow_very_late: bool = True):
        self.run_id = run_id
        self.r = random.Random(seed)
        self.sensors = U.sensors()
        self.asset = U.sensor_index()
        self.seq = defaultdict(int)
        self.stuck_value = None
        self.alarms = AlarmState()
        self.allow_very_late = allow_very_late

    def reading(self, tag: str, ts: datetime) -> dict:
        unit = U.UNITS[tag.split("-", 1)[0]]
        value = U.true_value(tag, ts, self.r)
        if tag == P["stuck_tag"] and ts >= STUCK_FROM:
            if self.stuck_value is None:
                self.stuck_value = value
            value = self.stuck_value
        if tag == P["unit_change_tag"] and ts >= UNIT_CHANGE_FROM:
            value, unit = round(value / BAR_TO_PSI, 4), "bar"
        self.seq[tag] += 1
        quality = "GOOD" if self.r.random() > 0.002 else "UNCERTAIN"
        return {"event_id": f"{tag}:{int(ts.timestamp() * 1000)}", "sensor_tag": tag, "event_ts": iso(ts),
                "value": value, "unit": unit, "quality": quality, "seq": self.seq[tag], "producer_run": self.run_id}

    def tick(self, ts: datetime) -> list[tuple[float, str, dict]]:
        """(send time, topic, message) for every tag at one instant, faults applied."""
        out = []
        for s in self.sensors:
            tag = s["sensor_tag"]
            jitter = self.r.uniform(0, 0.9)
            ets = ts + timedelta(seconds=jitter)
            msg = self.reading(tag, ets)
            delay = 0.0
            u = self.r.random()
            if u < P["late_fraction"]:
                delay = self.r.uniform(*P["late_seconds"])
            elif self.allow_very_late and u < P["late_fraction"] + 0.001:
                delay = self.r.uniform(15 * 60, MAX_DELAY_S)
            elif u < P["late_fraction"] + 0.001 + P["out_of_order_fraction"]:
                delay = self.r.uniform(1.0, 4.0)
            out.append((ets.timestamp() + delay, TOPIC["telemetry"], msg))
            design = self.asset[tag].design_pressure_psi
            for alarm in self.alarms.update(tag, msg["value"], msg["unit"], design, msg["event_ts"]):
                alarm["producer_run"] = self.run_id
                out.append((ets.timestamp() + delay + 0.5, TOPIC["alarm"], alarm))
        return out


# ---------------------------------------------------------------- sinks


class FileSink:
    """JSON lines per topic per hour of send time: <out>/<topic>/part-<yyyymmddHH>.jsonl, one Kafka
    record per line ({"key", "timestamp" (send time, ms), "value" (the message as a JSON string)}),
    so the streaming jobs read files and Kafka through the same parsing."""

    def __init__(self, out: str):
        self.out = Path(out)
        self.handles = {}
        self.sent = 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        hour = datetime.fromtimestamp(send_ts, timezone.utc).strftime("%Y%m%d%H")
        key = (topic, hour)
        if key not in self.handles:
            for k in [k for k in self.handles if k[0] == topic]:
                self.handles.pop(k).close()
            d = self.out / topic
            d.mkdir(parents=True, exist_ok=True)
            self.handles[key] = open(d / f"part-{hour}.jsonl", "a")
        rec = {"key": msg.get("sensor_tag") or msg.get("topic"), "timestamp": int(send_ts * 1000),
               "value": json.dumps(msg, separators=(",", ":"))}
        self.handles[key].write(json.dumps(rec, separators=(",", ":")) + "\n")
        self.sent += 1

    def close(self) -> None:
        for h in self.handles.values():
            h.close()


class KafkaSink:
    def __init__(self):
        from kafka import KafkaProducer

        from stream.producer.topics import kafka_config

        self.p = KafkaProducer(**kafka_config(), acks="all", linger_ms=50, batch_size=512 * 1024,
                               compression_type="gzip", retries=10,
                               value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
                               key_serializer=lambda k: k.encode() if k else None)
        self.sent = 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        self.p.send(topic, key=msg.get("sensor_tag") or msg.get("topic"), value=msg)
        self.sent += 1
        if self.sent % 100_000 == 0:
            self.p.flush()

    def close(self) -> None:
        self.p.flush()
        self.p.close()


# ---------------------------------------------------------------- control counts


class Control:
    """Counts per (event minute, topic, tag); a minute's control message goes out once no more
    of its events can still be sent (after the producer's maximum delay)."""

    def __init__(self, sink, run_id: str, hold_s: float):
        self.sink, self.run_id, self.hold_s = sink, run_id, hold_s
        self.counts = defaultdict(lambda: defaultdict(int))

    def count(self, topic: str, msg: dict) -> None:
        if topic == TOPIC["control"]:
            return
        minute = msg["event_ts"][:16] + ":00.000Z"
        self.counts[(minute, topic)][msg["sensor_tag"]] += 1

    def flush(self, now_ts: float, final: bool = False) -> int:
        n = 0
        for key in sorted(self.counts):
            minute, topic = key
            mts = datetime.strptime(minute, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc).timestamp()
            if final or mts + 60 + self.hold_s <= now_ts:
                c = self.counts.pop(key)
                self.sink.send(TOPIC["control"], {"minute": minute, "topic": topic, "counts": dict(c),
                                                  "total": sum(c.values()), "producer_run": self.run_id},
                               mts + 60 + self.hold_s)
                n += 1
        return n


# ---------------------------------------------------------------- modes


def backfill(sink, start: datetime, days: int, interval: int, run_id: str) -> None:
    g = Generator(run_id)
    ctl = Control(sink, run_id, MAX_DELAY_S + 5)
    pending: list[tuple[float, str, dict]] = []
    t, end = start, start + timedelta(days=days)
    last_report = time.time()
    while t < end:
        hour_end = t + timedelta(hours=1)
        while t < hour_end and t < end:
            pending += g.tick(t)
            t += timedelta(seconds=interval)
        pending.sort(key=lambda x: x[0])
        cutoff = t.timestamp()
        i = 0
        while i < len(pending) and pending[i][0] < cutoff:
            send_ts, topic, msg = pending[i]
            sink.send(topic, msg, send_ts)
            ctl.count(topic, msg)
            i += 1
        pending = pending[i:]
        ctl.flush(cutoff)
        if time.time() - last_report > 10:
            print(f"  {t.isoformat()} sent {sink.sent:,}", flush=True)
            last_report = time.time()
    for send_ts, topic, msg in sorted(pending, key=lambda x: x[0]):
        sink.send(topic, msg, send_ts)
        ctl.count(topic, msg)
    ctl.flush(0, final=True)


def live(sink, duration: int, interval: int, run_id: str) -> None:
    g = Generator(run_id, seed=int(time.time()), allow_very_late=False)
    ctl = Control(sink, run_id, max(P["late_seconds"]) + 5)
    pending: list[tuple[float, str, dict]] = []
    started = time.time()
    next_tick = started - started % interval + interval
    while duration == 0 or time.time() - started < duration:
        time.sleep(max(0.0, next_tick - time.time()))
        pending += g.tick(datetime.fromtimestamp(next_tick, timezone.utc))
        now = time.time()
        pending.sort(key=lambda x: x[0])
        while pending and pending[0][0] <= now:
            send_ts, topic, msg = pending.pop(0)
            sink.send(topic, msg, send_ts)
            ctl.count(topic, msg)
        ctl.flush(now)
        if int(next_tick) % 60 < interval:
            print(f"  {datetime.now(timezone.utc).isoformat(timespec='seconds')} sent {sink.sent:,}", flush=True)
        next_tick += interval
    for send_ts, topic, msg in pending:
        sink.send(topic, msg, send_ts)
        ctl.count(topic, msg)
    ctl.flush(0, final=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sink", choices=["kafka", "files"], default=os.environ.get("OGX_PRODUCER_SINK", "kafka"))
    ap.add_argument("--out", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--mode", choices=["backfill", "live"], default=os.environ.get("OGX_PRODUCER_MODE", "backfill"))
    ap.add_argument("--start-date", default=os.environ.get("OGX_PRODUCER_START", U.CFG["business_dates"][0]))
    ap.add_argument("--days", type=int, default=int(os.environ.get("OGX_PRODUCER_DAYS", len(U.CFG["business_dates"]))))
    ap.add_argument("--interval", type=int, default=None, help="seconds between readings per tag")
    ap.add_argument("--duration", type=int, default=int(os.environ.get("OGX_PRODUCER_DURATION", 0)))
    args, unknown = ap.parse_known_args(argv)
    if unknown:
        print(f"(ignoring arguments: {unknown})", flush=True)
    run_id = f"{args.mode}-{uuid.uuid4().hex[:8]}"
    if args.sink == "kafka" and os.environ.get("OGX_PRODUCER_SETUP") == "1":
        from stream.producer import topics

        print("topics:", topics.create_topics(), flush=True)
        try:
            print("schemas:", topics.register_schemas(), flush=True)
        except Exception as e:  # the stream does not depend on the registry; report and go on
            print(f"schemas: not registered ({type(e).__name__}: {str(e)[:200]})", flush=True)
    sink = FileSink(args.out) if args.sink == "files" else KafkaSink()
    t0 = time.time()
    try:
        if args.mode == "backfill":
            interval = args.interval or int(os.environ.get("OGX_PRODUCER_INTERVAL", 30))
            start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
            print(f"backfill {args.start_date} + {args.days} days at {interval} s, run {run_id}, sink {args.sink}",
                  flush=True)
            backfill(sink, start, args.days, interval, run_id)
        else:
            interval = args.interval or P["interval_s"]
            print(f"live at {interval} s per tag (~{len(U.sensors()) / interval:.0f} msg/s), run {run_id}", flush=True)
            live(sink, args.duration, interval, run_id)
    finally:
        sink.close()
    print(f"sent {sink.sent:,} messages in {time.time() - t0:.0f} s (run {run_id})", flush=True)
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
