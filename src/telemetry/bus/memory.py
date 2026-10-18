"""In-process partitioned log with Kafka-like semantics, for unit tests and the local demo."""

from __future__ import annotations

import threading
import zlib
from collections import defaultdict

from .base import BusUnavailable, Record


class MemoryBus:
    def __init__(self, partitions: int = 6):
        self.partitions = partitions
        self._log: dict[str, list[list[Record]]] = defaultdict(lambda: [[] for _ in range(self.partitions)])
        self._committed: dict[tuple[str, str, int], int] = defaultdict(int)  # (group, topic, partition) -> next offset
        self._lock = threading.Lock()
        self.available = True  # flip to simulate a network outage between plant and cloud

    # -- producer side -------------------------------------------------------
    def producer(self) -> MemoryProducer:
        return MemoryProducer(self)

    def _append(self, topic: str, key: str, value: bytes, headers: dict[str, str]) -> None:
        if not self.available:
            raise BusUnavailable("bus unreachable")
        p = zlib.crc32(key.encode()) % self.partitions  # same key -> same partition -> ordered
        with self._lock:
            part = self._log[topic][p]
            part.append(Record(topic, p, len(part), key, value, headers))

    # -- consumer side -------------------------------------------------------
    def consumer(self, group: str, topics: list[str]) -> MemoryConsumer:
        return MemoryConsumer(self, group, topics)

    def size(self, topic: str) -> int:
        return sum(len(p) for p in self._log[topic])


class MemoryProducer:
    def __init__(self, bus: MemoryBus):
        self.bus = bus

    def send(self, topic: str, key: str, value: bytes, headers: dict[str, str] | None = None) -> None:
        self.bus._append(topic, key, value, headers or {})

    def flush(self, timeout_s: float = 10.0) -> None:
        if not self.bus.available:
            raise BusUnavailable("bus unreachable")


class MemoryConsumer:
    def __init__(self, bus: MemoryBus, group: str, topics: list[str]):
        self.bus, self.group, self.topics = bus, group, topics
        self._position: dict[tuple[str, int], int] = {}

    def _pos(self, topic: str, p: int) -> int:
        return self._position.get((topic, p), self.bus._committed[(self.group, topic, p)])

    def poll_batch(self, max_records: int, timeout_s: float = 0.0) -> list[Record]:
        out: list[Record] = []
        with self.bus._lock:
            for topic in self.topics:
                for p, part in enumerate(self.bus._log[topic]):
                    start = self._pos(topic, p)
                    take = part[start : start + max(0, max_records - len(out))]
                    out.extend(take)
                    self._position[(topic, p)] = start + len(take)
        return out

    def commit(self, records: list[Record]) -> None:
        with self.bus._lock:
            for r in records:
                key = (self.group, r.topic, r.partition)
                self.bus._committed[key] = max(self.bus._committed[key], r.offset + 1)

    def rewind_to_committed(self) -> None:
        """What a restarted consumer does: continue from the last committed offset."""
        self._position.clear()

    def lag(self) -> int:
        with self.bus._lock:
            return sum(len(part) - self.bus._committed[(self.group, t, p)] for t in self.topics for p, part in enumerate(self.bus._log[t]))

    def close(self) -> None:
        pass
