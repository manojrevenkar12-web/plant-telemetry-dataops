"""Edge gateway: one per plant, between the OT network and the cloud.

Responsibilities, in order:
  1. parse what the devices send (MQTT payloads);
  2. bring legacy firmware onto the current contract (upcast v1 → v2) and validate;
  3. pseudonymise personal data — operator badge numbers never leave the plant (GDPR data minimisation);
  4. stamp gateway time and measure device clock skew;
  5. store-and-forward: every message is written to a local SQLite outbox first and only deleted
     after the bus acknowledges it, so a network outage delays data instead of losing it.
Messages that fail the contract are not dropped either: they go to the dead-letter topic with the reason.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ..bus.base import TOPIC_DLQ, TOPIC_TELEMETRY, BusUnavailable, Producer
from ..contracts import registry

SKEW_WARN_S = 120


@dataclass
class GatewayStats:
    received: int = 0
    forwarded: int = 0
    upcast_v1: int = 0
    to_dlq: int = 0
    pseudonymised: int = 0
    skew_flagged: int = 0
    dropped_overflow: int = 0
    forward_failures: int = 0
    buffered_peak: int = 0
    reasons: dict[str, int] = field(default_factory=dict)


class EdgeGateway:
    def __init__(
        self,
        plant_id: str,
        producer: Producer,
        outbox_path: Path | str = ":memory:",
        pseudonym_key: bytes = b"dev-only-key",
        max_buffer: int = 200_000,
    ):
        self.plant_id = plant_id
        self.gateway_id = f"gw-{plant_id.lower()}"
        self.producer = producer
        self.key = pseudonym_key
        self.max_buffer = max_buffer
        self.stats = GatewayStats()
        self.db = sqlite3.connect(str(outbox_path), check_same_thread=False)
        self.db.execute("pragma journal_mode=wal")
        # WAL + synchronous=NORMAL: every message is committed (survives a gateway crash or restart);
        # only an OS crash or power loss can lose the last few commits. Use FULL if that matters more than throughput.
        self.db.execute("pragma synchronous=normal")
        self.db.execute("create table if not exists outbox (id integer primary key autoincrement, topic text, key text, value blob, headers text)")
        self._count = self.db.execute("select count(*) from outbox").fetchone()[0]  # survives restarts

    # ---------------------------------------------------------------- ingest

    def handle(self, payload: bytes, now: datetime) -> None:
        """Process one device message. Never raises: bad input becomes a dead-letter record."""
        self.stats.received += 1
        try:
            msg = json.loads(payload)
            if not isinstance(msg, dict):
                raise ValueError("payload is not a JSON object")
        except (ValueError, UnicodeDecodeError) as exc:
            return self._dead_letter(payload, "malformed_json", str(exc)[:200])

        version = registry.detect_version(msg)
        if version == 1:
            problems = registry.errors(msg, 1)
            if problems:
                return self._dead_letter(payload, "contract_v1", problems[0])
            badge = msg.get("op")
            msg = registry.upcast_v1(msg)
            if badge:
                msg["operator_ref"] = self._pseudonym(badge)
            self.stats.upcast_v1 += 1
        elif version == 2:
            badge = msg.pop("operator_badge", None)
            if badge is not None:
                msg["operator_ref"] = self._pseudonym(badge)
        else:
            return self._dead_letter(payload, "unknown_contract", "no recognisable schema version")

        msg["gateway_id"] = self.gateway_id
        msg["gateway_time"] = now.isoformat()
        try:
            skew = (datetime.fromisoformat(msg["event_time"]) - now).total_seconds()
        except (KeyError, TypeError, ValueError):
            skew = 0.0
        msg["clock_skew_s"] = round(skew, 1)
        if abs(skew) > SKEW_WARN_S:
            self.stats.skew_flagged += 1

        problems = registry.errors(msg, 2)
        if problems:
            return self._dead_letter(json.dumps(msg).encode(), "contract_v2", problems[0])
        self._enqueue(TOPIC_TELEMETRY, msg["device_id"], json.dumps(msg, separators=(",", ":")).encode(), {"contract": "sensor_reading/2"})

    def _pseudonym(self, badge: str) -> str:
        # Keyed hash: stable per person (so shifts can be analysed) but not reversible without the plant key.
        self.stats.pseudonymised += 1
        return "op_" + hmac.new(self.key, badge.encode(), hashlib.sha256).hexdigest()[:16]

    def _dead_letter(self, payload: bytes, reason: str, detail: str) -> None:
        self.stats.to_dlq += 1
        self.stats.reasons[reason] = self.stats.reasons.get(reason, 0) + 1
        body = json.dumps({"gateway_id": self.gateway_id, "reason": reason, "detail": detail, "payload": payload.decode("utf-8", "replace")[:4000]})
        self._enqueue(TOPIC_DLQ, self.gateway_id, body.encode(), {"reason": reason})

    # ---------------------------------------------------------------- store and forward

    def _enqueue(self, topic: str, key: str, value: bytes, headers: dict[str, str]) -> None:
        self.db.execute("insert into outbox (topic, key, value, headers) values (?, ?, ?, ?)", (topic, key, value, json.dumps(headers)))
        self._count += 1
        n = self._count
        if n > self.max_buffer:
            # Bounded disk: drop the oldest and count it, so the loss is visible in monitoring.
            over = n - self.max_buffer
            self.db.execute("delete from outbox where id in (select id from outbox order by id limit ?)", (over,))
            self.stats.dropped_overflow += over
            self._count -= over
        self.db.commit()
        self.stats.buffered_peak = max(self.stats.buffered_peak, min(n, self.max_buffer))

    def buffered(self) -> int:
        return self._count

    def forward(self, batch_size: int = 5_000) -> int:
        """Send buffered messages in order; delete them only after the bus confirms. Returns messages sent."""
        sent = 0
        while True:
            rows = self.db.execute("select id, topic, key, value, headers from outbox order by id limit ?", (batch_size,)).fetchall()
            if not rows:
                return sent
            try:
                for _, topic, key, value, headers in rows:
                    self.producer.send(topic, key, value, json.loads(headers))
                self.producer.flush()
            except BusUnavailable:
                self.stats.forward_failures += 1
                return sent  # keep everything; the idempotent pipeline tolerates re-sends after recovery
            self.db.execute("delete from outbox where id <= ?", (rows[-1][0],))
            self.db.commit()
            self._count -= len(rows)
            sent += len(rows)
            self.stats.forwarded += len(rows)
