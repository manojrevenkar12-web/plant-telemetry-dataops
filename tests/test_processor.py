import json
from datetime import timedelta

import polars as pl
import pytest

from telemetry.bus.base import TOPIC_DLQ, TOPIC_TELEMETRY
from telemetry.bus.memory import MemoryBus
from telemetry.edge import devices
from telemetry.lake.tables import Lake
from telemetry.stream.processor import ALLOWED_LATENESS, StreamProcessor

from .conftest import encode, v2

T0 = devices.SHIFT_START


@pytest.fixture
def env(tmp_path):
    bus = MemoryBus()
    consumer = bus.consumer("proc", [TOPIC_TELEMETRY, TOPIC_DLQ])
    lake = Lake(tmp_path / "lake")
    return bus, bus.producer(), consumer, lake, StreamProcessor(consumer, lake)


def send(p, m):
    p.send(TOPIC_TELEMETRY, m["device_id"], encode(m | {"gateway_id": "gw", "gateway_time": m["event_time"], "clock_skew_s": 0.0}))


def test_redelivered_messages_are_stored_once(env):
    _, p, _, lake, proc = env
    for i in range(6):
        send(p, v2(seq=i, event_time=T0 + timedelta(seconds=10 * i)))
    send(p, v2(seq=2, event_time=T0 + timedelta(seconds=20)))  # QoS-1 duplicate in the same batch
    r1 = proc.run_once(now=T0 + timedelta(minutes=1))
    for i in (3, 4):  # gateway re-send after an outage, in a later batch
        send(p, v2(seq=i, event_time=T0 + timedelta(seconds=10 * i)))
    r2 = proc.run_once(now=T0 + timedelta(minutes=2))
    assert (r1.to_silver, r1.duplicates, r2.to_silver, r2.duplicates) == (6, 1, 0, 2)
    assert lake.read("silver/readings").height == 6
    gold = lake.read("gold/metrics_1m")
    assert gold.height == 1 and gold["n"][0] == 6


def test_crash_before_commit_replays_without_duplicates(env):
    _, p, consumer, lake, proc = env
    for i in range(30):
        send(p, v2(seq=i, event_time=T0 + timedelta(seconds=10 * i)))
    proc.run_once(now=T0 + timedelta(minutes=6))
    gold_before = lake.read("gold/metrics_1m").sort("minute")
    # simulate a crash after the writes but before the commit: offsets roll back, everything is redelivered
    consumer.bus._committed.clear()
    consumer.rewind_to_committed()
    res = proc.run_once(now=T0 + timedelta(minutes=7))
    assert res.records == 30 and res.to_silver == 0 and res.duplicates == 30
    assert lake.read("silver/readings").height == 30
    assert lake.read("gold/metrics_1m").sort("minute").equals(gold_before)


def test_late_reading_is_flagged_and_corrects_its_window(env):
    _, p, _, lake, proc = env
    send(p, v2(seq=0, value=2.0, event_time=T0))
    proc.run_once(now=T0 + timedelta(minutes=1))
    send(p, v2(seq=100, value=2.0, event_time=T0 + timedelta(minutes=30)))
    proc.run_once(now=T0 + timedelta(minutes=31))
    send(p, v2(seq=1, value=4.0, event_time=T0 + timedelta(seconds=10)))  # arrives 30 min late
    res = proc.run_once(now=T0 + timedelta(minutes=32))
    assert res.late == 1 and proc.watermark == T0 + timedelta(minutes=30) - ALLOWED_LATENESS
    w = lake.read("gold/metrics_1m").filter(pl.col("minute") == T0)
    assert w["n"][0] == 2 and w["avg"][0] == 3.0 and w["late_readings"][0] == 1


def test_drifting_clock_does_not_move_the_watermark(env):
    _, p, _, _, proc = env
    m = v2(seq=0, event_time=T0 + timedelta(minutes=10))
    p.send(TOPIC_TELEMETRY, m["device_id"], encode(m | {"clock_skew_s": 600.0}))
    send(p, v2(device="PL01-L1-TMP-01", metric="fryer_temp_c", value=175.0, event_time=T0))
    proc.run_once(now=T0 + timedelta(minutes=1))
    assert proc.watermark == T0 - ALLOWED_LATENESS


def test_out_of_range_values_are_kept_but_excluded_from_aggregates(env):
    _, p, _, lake, proc = env
    send(p, v2(device="PL01-L3-TMP-01", metric="fryer_temp_c", value=175.0, seq=0))
    send(p, v2(device="PL01-L3-TMP-01", metric="fryer_temp_c", value=9999.0, seq=1, quality="BAD", event_time=T0 + timedelta(seconds=10)))
    res = proc.run_once(now=T0 + timedelta(minutes=1))
    assert res.out_of_range == 1 and lake.read("silver/readings").height == 2
    g = lake.read("gold/metrics_1m")
    assert g["avg"][0] == 175.0 and g["bad_readings"][0] == 1


def test_invalid_and_impossible_events_become_dead_letters(env):
    _, p, _, lake, proc = env
    p.send(TOPIC_TELEMETRY, "x", b"not json")
    send(p, v2(seq=0, event_time=T0 + timedelta(hours=2)))
    send(p, v2(seq=1, event_time=T0 - timedelta(days=30)))
    p.send(TOPIC_DLQ, "gw-pl01", json.dumps({"gateway_id": "gw-pl01", "reason": "contract_v1", "detail": "x", "payload": "{}"}).encode())
    res = proc.run_once(now=T0)
    assert res.reasons == {"malformed_json": 1, "event_time_in_future": 1, "event_time_too_old": 1, "contract_v1": 1}
    assert lake.read("bronze/dead_letters").height == 4 and not lake.exists("silver/readings")
    assert lake.read("bronze/telemetry_raw").height == 4  # bronze keeps everything for replay
