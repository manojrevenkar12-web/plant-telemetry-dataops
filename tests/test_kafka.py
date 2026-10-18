"""Integration test against a real Kafka broker (runs in CI with an apache/kafka service container)."""

import os
import uuid
from datetime import timedelta

import pytest

pytestmark = pytest.mark.kafka

if not os.environ.get("KAFKA_BOOTSTRAP"):
    pytest.skip("KAFKA_BOOTSTRAP not set", allow_module_level=True)

from telemetry.bus.kafka import KafkaConsumer, KafkaProducer, ensure_topics  # noqa: E402
from telemetry.edge import devices  # noqa: E402
from telemetry.edge.gateway import EdgeGateway  # noqa: E402
from telemetry.lake.tables import Lake  # noqa: E402
from telemetry.stream.processor import StreamProcessor  # noqa: E402


def test_gateway_to_lake_through_kafka(tmp_path, monkeypatch):
    import telemetry.bus.base as base
    import telemetry.edge.gateway as gateway_mod
    import telemetry.stream.processor as proc_mod

    suffix = uuid.uuid4().hex[:8]
    telemetry_topic, dlq_topic = f"plant.telemetry.v2.{suffix}", f"plant.telemetry.dlq.{suffix}"
    for mod in (base, gateway_mod):
        monkeypatch.setattr(mod, "TOPIC_TELEMETRY", telemetry_topic, raising=False)
        monkeypatch.setattr(mod, "TOPIC_DLQ", dlq_topic, raising=False)
    monkeypatch.setattr(proc_mod, "TOPIC_DLQ", dlq_topic)
    ensure_topics([telemetry_topic, dlq_topic], partitions=3)

    gw = EdgeGateway("PL01", KafkaProducer())
    ems = [e for e in devices.simulate(10) if e.plant_id == "PL01"]
    for e in ems:
        gw.handle(e.payload, e.sent_at)
    sent = gw.forward()
    assert sent == len(ems) and gw.buffered() == 0

    lake = Lake(tmp_path / "lake")
    group = f"test-{suffix}"
    proc = StreamProcessor(KafkaConsumer(group, [telemetry_topic, dlq_topic]), lake)
    now = devices.SHIFT_START + timedelta(minutes=11)
    consumed = 0
    for _ in range(30):
        res = proc.run_once(now=now)
        consumed += res.records
        if consumed >= sent:
            break
    assert consumed == sent
    silver = lake.read("silver/readings")
    assert silver.height == silver.select(["device_id", "metric", "seq"]).unique().height
    proc.consumer.close()

    # committed offsets: a new consumer in the same group gets nothing new
    again = KafkaConsumer(group, [telemetry_topic, dlq_topic])
    assert again.poll_batch(1000, timeout_s=5) == []
    again.close()
