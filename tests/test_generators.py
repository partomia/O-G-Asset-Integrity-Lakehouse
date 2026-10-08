"""Generators: one consistent universe, deterministic files, manifests that match, planted faults present."""
import hashlib
import json
from datetime import date

from generators import asset_universe as U
from generators import faults, gen_documents, gen_erp, gen_videos
from generators.land import batch
from ogx_common import dump_columns, parse_dump, psv_stats

D1, D2, D3 = date(2026, 10, 4), date(2026, 10, 5), date(2026, 10, 6)


def test_universe_size_and_sensor_tags():
    xs = U.assets()
    assert len(xs) == 222
    assert {a.facility_id for a in xs} == {"FLD-01", "PL-03", "REF-U1"}
    assert sum(len(a.sensor_tags) for a in xs) == U.CFG["universe"]["sensor_tags"] == 200
    assert len({a.equnr for a in xs}) == 222 and len({a.tag for a in xs}) == 222
    for tag in U.DEGRADATION:
        assert tag in U.by_tag()


def test_degradation_truth_moves_the_signal():
    from datetime import datetime, timezone

    early = datetime(2026, 9, 1, tzinfo=timezone.utc)
    late = datetime(2026, 10, 12, tzinfo=timezone.utc)
    assert U.true_value("VI-105A.PV", late) > U.true_value("VI-105A.PV", early) + 3
    assert U.event_within("PL-03-SEG-027", 0.0) and not U.event_within("P-101A", 0.0)


def test_batch_is_deterministic_and_manifest_matches():
    a, b = batch(D1, ["erp", "seismic"]), batch(D1, ["erp", "seismic"])
    assert a == b
    for source, files in a.items():
        m = json.loads(dict(files)["_manifest.json"])
        landed = {n: x for n, x in files if n != "_manifest.json"}
        assert m["file_count"] == len(landed)
        for f in m["files"]:
            assert hashlib.sha256(landed[f["name"]]).hexdigest() == f["sha256"]
            assert len(landed[f["name"]]) == f["size"]


def test_erp_dump_parses_every_asset():
    text = gen_erp.asset_register_dump(D1).decode()
    cols = dump_columns(text, "equi")
    assert cols == gen_erp.EQUI_COLUMNS
    rows = list(parse_dump("x.sql", text, "equi", cols))
    assert len(rows) == 222


def test_work_order_trailer_fault():
    ok = psv_stats("a.psv", gen_erp.work_orders_psv(D2).decode())
    off = psv_stats("b.psv", gen_erp.work_orders_psv(D3).decode())
    assert ok[2] == ok[3]
    assert off[3] == off[2] + 1 and faults.WO_TRAILER_OFF_DATE == D3.isoformat()


def test_planted_faults_in_documents_and_videos():
    plan = gen_documents.inspection_plan(D2)
    assert sum(p["corrupt"] for p in plan) == 1
    assert any(p["scanned"] for p in plan)
    vids = gen_videos.video_plan(D3)
    wrong = [v for v in vids if v["named_tag"] != v["asset_tag"]]
    assert len(wrong) == 1 and wrong[0]["asset_tag"] == faults.WRONG_VIDEO_TAG["true_tag"]


def test_seismic_resend_is_byte_identical():
    files = dict((n, x) for n, x in batch(D2, ["seismic"])["seismic"])
    first = dict((n, x) for n, x in batch(D1, ["seismic"])["seismic"])
    assert files["OGX_2D_L101_resend.sgy"] == first["OGX_2D_L101.sgy"]
