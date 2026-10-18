"""Message bus interface. Two implementations share it:

* `KafkaBus` — Apache Kafka or Azure Event Hubs (Kafka protocol); used in production and in CI integration tests.
* `MemoryBus` — an in-process partitioned log with consumer-group offsets; used for fast unit tests and the local demo.

Semantics the pipeline relies on (both implementations provide them): per-key ordering within a
partition, at-least-once delivery, and offsets that only advance when the consumer commits.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

TOPIC_TELEMETRY = "plant.telemetry.v2"
TOPIC_DLQ = "plant.telemetry.dlq"


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)


class BusUnavailable(RuntimeError):
    """The bus cannot be reached (network outage, broker down). Producers must keep the data."""


class Producer(Protocol):
    def send(self, topic: str, key: str, value: bytes, headers: dict[str, str] | None = None) -> None: ...

    def flush(self, timeout_s: float = 10.0) -> None: ...


class Consumer(Protocol):
    def poll_batch(self, max_records: int, timeout_s: float) -> list[Record]: ...

    def commit(self, records: list[Record]) -> None: ...

    def lag(self) -> int: ...

    def close(self) -> None: ...
