"""Streaming processor: bus → bronze → silver → gold, in micro-batches.

Delivery guarantee: at-least-once from the bus, made effectively exactly-once in the lake:
  * raw records are appended to bronze with their topic/partition/offset (full replay possible);
  * silver is a MERGE on (device_id, metric, seq), so redeliveries and gateway re-sends are ignored;
  * gold minute aggregates are recomputed from silver for every minute the batch touched, so a late
    reading corrects its window instead of creating a second one;
  * offsets are committed only after all three writes succeed — a crash means re-processing, never loss.

Event time vs processing time: readings carry the device's event time. A watermark (latest event
time seen minus the allowed lateness) marks late readings; they are still stored and still
correct the aggregates, but are counted so a growing backlog is visible.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import polars as pl

from ..bus.base import TOPIC_DLQ, Consumer, Record
from ..contracts import registry
from ..lake.tables import Lake

ALLOWED_LATENESS = timedelta(minutes=15)
MAX_FUTURE_SKEW = timedelta(minutes=15)
MAX_AGE = timedelta(days=7)
CLOCK_TRUST_S = 120  # same threshold the gateway uses to flag skew
RANGES = {  # physically plausible ranges per metric; outside = flagged, not dropped
    "fryer_temp_c": (100.0, 220.0),
    "vibration_mm_s": (0.0, 50.0),
    "line_speed_m_min": (0.0, 120.0),
    "power_kw": (0.0, 400.0),
    "good_count": (0.0, 500.0),
    "reject_count": (0.0, 500.0),
}


@dataclass
class BatchResult:
    records: int = 0
    to_silver: int = 0
    duplicates: int = 0
    dead_letters: int = 0
    late: int = 0
    out_of_range: int = 0
    gold_windows: int = 0
    seconds: float = 0.0
    reasons: dict[str, int] = field(default_factory=dict)


class StreamProcessor:
    def __init__(self, consumer: Consumer, lake: Lake, metrics=None):
        self.consumer = consumer
        self.lake = lake
        self.metrics = metrics
        self.watermark: datetime | None = None
        self.max_event_time: datetime | None = None

    def run_once(self, max_records: int = 20_000, now: datetime | None = None) -> BatchResult:
        t0 = time.perf_counter()
        records = self.consumer.poll_batch(max_records, timeout_s=1.0)
        res = BatchResult(records=len(records))
        if not records:
            return res
        now = now or datetime.now(UTC)

        # 1. bronze: everything, raw, with its bus coordinates
        self.lake.append(
            "bronze/telemetry_raw",
            [
                {
                    "topic": r.topic,
                    "partition": r.partition,
                    "offset": r.offset,
                    "key": r.key,
                    "value": r.value.decode("utf-8", "replace"),
                    "contract": r.headers.get("contract", ""),
                    "ingested_at": now,
                    "ingest_date": now.date(),
                }
                for r in records
            ],
        )

        # 2. parse + validate; dead letters keep the reason
        rows, dead = [], []
        for r in records:
            if r.topic == TOPIC_DLQ:
                body = _json(r.value) or {}
                dead.append(
                    {
                        "source": body.get("gateway_id", r.key),
                        "reason": body.get("reason", "edge"),
                        "detail": body.get("detail", ""),
                        "payload": body.get("payload", ""),
                        "received_at": now,
                    }
                )
                continue
            row, reason = self._to_row(r, now)
            if reason:
                dead.append(
                    {
                        "source": "stream-processor",
                        "reason": reason,
                        "detail": "",
                        "payload": r.value.decode("utf-8", "replace")[:4000],
                        "received_at": now,
                    }
                )
            else:
                rows.append(row)
        for d in dead:
            res.reasons[d["reason"]] = res.reasons.get(d["reason"], 0) + 1
        res.dead_letters = len(dead)
        self.lake.append("bronze/dead_letters", dead)

        # 3. watermark and lateness (event time)
        if rows:
            # A device with a drifting clock must not drag the watermark forward and make everyone else
            # look late, so only readings whose clock agrees with the gateway advance it.
            trusted = [x["event_time"] for x in rows if abs(x["clock_skew_s"] or 0) <= CLOCK_TRUST_S] or [x["event_time"] for x in rows]
            batch_max = max(trusted)
            self.max_event_time = max(self.max_event_time or batch_max, batch_max)
            self.watermark = self.max_event_time - ALLOWED_LATENESS
            for x in rows:
                x["is_late"] = x["event_time"] < self.watermark
            res.late = sum(x["is_late"] for x in rows)
            res.out_of_range = sum(not x["in_range"] for x in rows)

        # 4. silver: dedup within the batch, then insert-only merge (first version wins)
        uniq: dict[tuple, dict] = {}
        for x in rows:
            uniq.setdefault((x["device_id"], x["metric"], x["seq"]), x)
        res.duplicates = len(rows) - len(uniq)
        merged = self.lake.merge("silver/readings", list(uniq.values()), update=False)
        inserted = merged.get("num_target_rows_inserted", 0)
        res.duplicates += len(uniq) - inserted
        res.to_silver = inserted

        # 5. gold: recompute every (device, metric, minute) the batch touched, from silver
        if uniq:
            res.gold_windows = self._refresh_gold(list(uniq.values()))

        # 6. only now is the batch safe to acknowledge
        self.consumer.commit(records)
        res.seconds = round(time.perf_counter() - t0, 3)
        if self.metrics:
            self.metrics.observe_batch(res, self.consumer.lag(), self.watermark, now)
        return res

    def _to_row(self, r: Record, now: datetime) -> tuple[dict | None, str | None]:
        msg = _json(r.value)
        if msg is None:
            return None, "malformed_json"
        if registry.errors(msg, 2):
            return None, "contract_v2"
        et = datetime.fromisoformat(msg["event_time"]).astimezone(UTC)
        if et > now + MAX_FUTURE_SKEW:
            return None, "event_time_in_future"
        if et < now - MAX_AGE:
            return None, "event_time_too_old"
        lo, hi = RANGES[msg["metric"]]
        gw = datetime.fromisoformat(msg["gateway_time"]).astimezone(UTC) if msg.get("gateway_time") else None
        return {
            "message_id": msg["message_id"],
            "device_id": msg["device_id"],
            "plant_id": msg["plant_id"],
            "line_id": msg["line_id"],
            "metric": msg["metric"],
            "value": float(msg["value"]),
            "unit": msg["unit"],
            "event_time": et,
            "event_date": et.date(),
            "seq": msg["seq"],
            "quality": msg["quality"],
            "firmware": msg.get("firmware"),
            "operator_ref": msg.get("operator_ref"),
            "gateway_id": msg.get("gateway_id"),
            "gateway_time": gw,
            "clock_skew_s": msg.get("clock_skew_s"),
            "arrival_delay_s": (now - et).total_seconds(),
            "is_late": False,
            "in_range": msg["quality"] == "GOOD" and lo <= float(msg["value"]) <= hi,
            "processed_at": now,
        }, None

    def _refresh_gold(self, rows: list[dict]) -> int:
        touched = pl.DataFrame(
            [{"device_id": x["device_id"], "metric": x["metric"], "minute": x["event_time"].replace(second=0, microsecond=0)} for x in rows]
        ).unique()
        dates = sorted({x["event_date"] for x in rows})
        devices = sorted(touched["device_id"].unique().to_list())
        silver = self.lake.read("silver/readings", filters=[("event_date", "in", dates), ("device_id", "in", devices)])
        silver = silver.with_columns(pl.col("event_time").dt.truncate("1m").alias("minute"))
        agg = (
            silver.join(touched, on=["device_id", "metric", "minute"], how="inner")
            .group_by(["device_id", "plant_id", "line_id", "metric", "minute"])
            .agg(
                pl.len().alias("n"),
                pl.col("value").filter(pl.col("in_range")).mean().alias("avg"),
                pl.col("value").filter(pl.col("in_range")).min().alias("min"),
                pl.col("value").filter(pl.col("in_range")).max().alias("max"),
                pl.col("value").filter(pl.col("in_range")).sum().alias("sum"),
                (~pl.col("in_range")).sum().cast(pl.Int64).alias("bad_readings"),
                pl.col("is_late").sum().cast(pl.Int64).alias("late_readings"),
            )
            .with_columns(pl.lit("stream").alias("source"), pl.col("minute").dt.date().alias("event_date"))
        )
        self.lake.merge("gold/metrics_1m", agg.to_arrow())
        return agg.height


def _json(raw: bytes) -> dict | None:
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else None
    except (ValueError, UnicodeDecodeError):
        return None
