import json
from datetime import timedelta

from telemetry.bus.base import TOPIC_DLQ, TOPIC_TELEMETRY
from telemetry.bus.link import Link
from telemetry.bus.memory import MemoryBus
from telemetry.edge import devices
from telemetry.edge.gateway import EdgeGateway

from .conftest import encode, v2

T0 = devices.SHIFT_START


def _gw(tmp_path=None, **kw):
    bus = MemoryBus()
    link = Link(bus.producer())
    gw = EdgeGateway("PL01", link, (tmp_path / "outbox.db") if tmp_path else ":memory:", pseudonym_key=b"k1", **kw)
    return bus, link, gw


def _all(bus, topic):
    return [json.loads(r.value) for r in bus.consumer("t", [topic]).poll_batch(10_000)]


def test_operator_badge_is_pseudonymised_before_leaving_the_plant():
    bus, _, gw = _gw()
    gw.handle(encode(v2(metric="good_count", operator_badge="B-100017")), T0)
    gw.handle(encode(v2(metric="good_count", seq=1, operator_badge="B-100017")), T0)
    gw.forward()
    msgs = _all(bus, TOPIC_TELEMETRY)
    raw = b"".join(r.value for r in bus.consumer("x", [TOPIC_TELEMETRY]).poll_batch(100))
    assert b"B-100017" not in raw and "operator_badge" not in msgs[0]
    assert msgs[0]["operator_ref"] == msgs[1]["operator_ref"] and msgs[0]["operator_ref"].startswith("op_")
    other = EdgeGateway("PL01", MemoryBus().producer(), pseudonym_key=b"another-plant-key")
    assert other._pseudonym("B-100017") != msgs[0]["operator_ref"]


def test_bad_messages_go_to_dead_letter_with_reason():
    bus, _, gw = _gw()
    gw.handle(b'{"schema_version": 2, "message', T0)
    gw.handle(encode(v2(metric="humidity_pct")), T0)
    gw.handle(encode({"hello": "world"}), T0)
    gw.forward()
    reasons = sorted(m["reason"] for m in _all(bus, TOPIC_DLQ))
    assert reasons == ["contract_v2", "malformed_json", "unknown_contract"]
    assert bus.size(TOPIC_TELEMETRY) == 0


def test_store_and_forward_survives_an_outage_without_loss(tmp_path):
    bus, link, gw = _gw(tmp_path)
    link.up = False
    for i in range(500):
        gw.handle(encode(v2(seq=i)), T0 + timedelta(seconds=i))
    assert gw.forward() == 0 and gw.buffered() == 500 and gw.stats.forward_failures == 1
    # the gateway restarts during the outage: the outbox is on disk
    gw2 = EdgeGateway("PL01", link, tmp_path / "outbox.db", pseudonym_key=b"k1")
    assert gw2.buffered() == 500
    link.up = True
    assert gw2.forward() == 500 and gw2.buffered() == 0
    assert [m["seq"] for m in _all(bus, TOPIC_TELEMETRY)] == list(range(500))


def test_full_outbox_drops_oldest_and_counts_it():
    _, link, gw = _gw(max_buffer=100)
    link.up = False
    for i in range(150):
        gw.handle(encode(v2(seq=i)), T0)
    assert gw.buffered() == 100 and gw.stats.dropped_overflow == 50


def test_clock_skew_is_measured_and_flagged():
    bus, _, gw = _gw()
    gw.handle(encode(v2(event_time=T0 + timedelta(minutes=7))), T0)
    gw.forward()
    msg = _all(bus, TOPIC_TELEMETRY)[0]
    assert msg["clock_skew_s"] == 420.0 and gw.stats.skew_flagged == 1
