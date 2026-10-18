"""Apache Kafka / Azure Event Hubs (Kafka protocol) bus.

Event Hubs speaks the Kafka protocol on port 9093, so the same code runs against both. Settings:

    KAFKA_BOOTSTRAP=localhost:9092                                   # local Kafka
    KAFKA_BOOTSTRAP=<namespace>.servicebus.windows.net:9093           # Event Hubs
    KAFKA_SECURITY_PROTOCOL=SASL_SSL  KAFKA_SASL_MECHANISM=PLAIN
    KAFKA_SASL_USERNAME='$ConnectionString'  KAFKA_SASL_PASSWORD=<from Key Vault>

In Azure the preferred option is Entra ID (OAUTHBEARER with a managed identity) instead of a
connection string; that is a configuration change, not a code change.
"""

from __future__ import annotations

import os

from confluent_kafka import Consumer as _Consumer
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka import Producer as _Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .base import BusUnavailable, Record


def client_config() -> dict:
    cfg = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}
    if proto := os.environ.get("KAFKA_SECURITY_PROTOCOL"):
        cfg["security.protocol"] = proto
    if mech := os.environ.get("KAFKA_SASL_MECHANISM"):
        cfg |= {"sasl.mechanism": mech, "sasl.username": os.environ["KAFKA_SASL_USERNAME"], "sasl.password": os.environ["KAFKA_SASL_PASSWORD"]}
    return cfg


def ensure_topics(topics: list[str], partitions: int = 6, replication: int = 1) -> None:
    admin = AdminClient(client_config())
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=replication) for t in topics if t not in existing]
    for fut in admin.create_topics(new).values() if new else []:
        try:
            fut.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


class KafkaProducer:
    def __init__(self):
        # Idempotent producer: retries never create duplicates in the log; acks=all waits for in-sync replicas.
        # KAFKA_IDEMPOTENCE=false for brokers without idempotent-producer support; the lake dedups either way.
        idempotent = os.environ.get("KAFKA_IDEMPOTENCE", "true").lower() != "false"
        self._p = _Producer(client_config() | {"enable.idempotence": idempotent, "acks": "all", "compression.type": "lz4", "linger.ms": 20})
        self._errors: list[str] = []

    def _delivery(self, err, _msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def send(self, topic: str, key: str, value: bytes, headers: dict[str, str] | None = None) -> None:
        try:
            self._p.produce(topic, key=key, value=value, headers=list((headers or {}).items()), on_delivery=self._delivery)
        except BufferError as exc:
            raise BusUnavailable("local producer queue full") from exc
        self._p.poll(0)

    def flush(self, timeout_s: float = 10.0) -> None:
        remaining = self._p.flush(timeout_s)
        if remaining or self._errors:
            errs, self._errors = self._errors, []
            raise BusUnavailable(f"{remaining} messages undelivered; {errs[:3]}")


class KafkaConsumer:
    def __init__(self, group: str, topics: list[str]):
        self._c = _Consumer(
            client_config() | {"group.id": group, "enable.auto.commit": False, "auto.offset.reset": "earliest", "isolation.level": "read_committed"}
        )
        self._c.subscribe(topics)
        self.topics = topics

    def poll_batch(self, max_records: int, timeout_s: float = 1.0) -> list[Record]:
        msgs = self._c.consume(num_messages=max_records, timeout=timeout_s)
        out = []
        for m in msgs:
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(m.error())
            out.append(
                Record(m.topic(), m.partition(), m.offset(), (m.key() or b"").decode(), m.value(), {k: v.decode() for k, v in (m.headers() or [])})
            )
        return out

    def commit(self, records: list[Record]) -> None:
        latest: dict[tuple[str, int], int] = {}
        for r in records:
            latest[(r.topic, r.partition)] = max(latest.get((r.topic, r.partition), -1), r.offset)
        if latest:
            self._c.commit(offsets=[TopicPartition(t, p, o + 1) for (t, p), o in latest.items()], asynchronous=False)

    def lag(self) -> int:
        total = 0
        for tp in self._c.assignment():
            low, high = self._c.get_watermark_offsets(tp, timeout=5)
            committed = self._c.committed([tp], timeout=5)[0].offset
            total += high - (committed if committed >= 0 else low)
        return total

    def close(self) -> None:
        self._c.close()
