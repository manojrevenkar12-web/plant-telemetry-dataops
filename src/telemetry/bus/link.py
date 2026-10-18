"""A plant's network link to the cloud. In the simulation it can be cut to test store-and-forward."""

from __future__ import annotations

from .base import BusUnavailable, Producer


class Link:
    def __init__(self, producer: Producer):
        self.producer = producer
        self.up = True

    def send(self, topic: str, key: str, value: bytes, headers: dict[str, str] | None = None) -> None:
        if not self.up:
            raise BusUnavailable("plant network down")
        self.producer.send(topic, key, value, headers)

    def flush(self, timeout_s: float = 10.0) -> None:
        if not self.up:
            raise BusUnavailable("plant network down")
        self.producer.flush(timeout_s)
