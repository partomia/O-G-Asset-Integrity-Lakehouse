"""Producer: control counts reconcile with what was sent, and the planted stream faults are present."""
import json
from collections import Counter
from datetime import datetime, timedelta, timezone

from stream.producer import sensor_producer as SP
from stream.producer.alarm_rules import AlarmState


def _read(tmp_path, topic):
    out = []
    for f in sorted((tmp_path / topic).glob("*.jsonl")):
        out += [json.loads(line) for line in f.read_text().splitlines()]
    return out


def _produce(tmp_path, start: str, hours: int) -> None:
    sink = SP.FileSink(str(tmp_path))
    g = SP.Generator("test")
    ctl = SP.Control(sink, "test", SP.MAX_DELAY_S + 5)
    t = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end = t + timedelta(hours=hours)
    msgs = []
    while t < end:
        msgs += g.tick(t)
        t += timedelta(seconds=30)
    for send_ts, topic, m in sorted(msgs, key=lambda x: x[0]):
        sink.send(topic, m, send_ts)
        ctl.count(topic, m)
    ctl.flush(0, final=True)
    sink.close()


def test_control_counts_match_messages(tmp_path):
    _produce(tmp_path, "2026-10-04T00:00:00", 1)
    readings = _read(tmp_path, "ogx.sensor.telemetry")
    sent = Counter((m["event_ts"][:16], m["sensor_tag"]) for m in readings)
    ctl = Counter()
    for c in _read(tmp_path, "ogx.control"):
        if c["topic"] == "ogx.sensor.telemetry":
            for tag, n in c["counts"].items():
                ctl[(c["minute"][:16], tag)] += n
    assert sent == ctl
    assert len({m["sensor_tag"] for m in readings}) == 200


def test_stuck_sensor_and_unit_change(tmp_path):
    _produce(tmp_path, "2026-10-06T09:30:00", 1)
    readings = _read(tmp_path, "ogx.sensor.telemetry")
    assert len({m["value"] for m in readings if m["sensor_tag"] == SP.P["stuck_tag"]}) == 1
    assert {m["unit"] for m in readings if m["sensor_tag"] == SP.P["unit_change_tag"]} == {"psi", "bar"}


def test_alarm_hysteresis():
    s = AlarmState()
    ev = s.update("VI-105A.PV", 5.0, "mm/s", None, "t1") + s.update("VI-105A.PV", 4.6, "mm/s", None, "t2")
    assert [e["state"] for e in ev] == ["ACTIVE"]
    assert [e["state"] for e in s.update("VI-105A.PV", 3.0, "mm/s", None, "t3")] == ["CLEARED"]
