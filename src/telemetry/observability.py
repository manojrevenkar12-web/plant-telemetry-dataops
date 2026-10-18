"""Prometheus metrics for the pipeline. The alert rules in ops/prometheus/alerts.yml use these names."""

from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class PipelineMetrics:
    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.records = Counter("telemetry_records_consumed_total", "Records read from the bus", registry=r)
        self.silver = Counter("telemetry_silver_rows_total", "New rows written to silver/readings", registry=r)
        self.duplicates = Counter("telemetry_duplicates_total", "Redelivered or re-sent readings dropped by dedup", registry=r)
        self.dead = Counter("telemetry_dead_letters_total", "Records sent to the dead-letter table", ["reason"], registry=r)
        self.late = Counter("telemetry_late_readings_total", "Readings older than the watermark", registry=r)
        self.out_of_range = Counter("telemetry_out_of_range_total", "Readings outside the physical range or flagged BAD", registry=r)
        self.lag = Gauge("telemetry_consumer_lag_records", "Records on the bus not yet committed by the processor", registry=r)
        self.watermark_delay = Gauge("telemetry_watermark_delay_seconds", "Wall clock minus event-time watermark", registry=r)
        self.batch_seconds = Histogram(
            "telemetry_batch_duration_seconds", "Micro-batch duration", buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10), registry=r
        )
        self.gateway_buffered = Gauge("telemetry_gateway_buffered_messages", "Messages waiting in a plant gateway outbox", ["plant"], registry=r)
        self.gateway_dropped = Gauge(
            "telemetry_gateway_dropped_total", "Messages a gateway dropped because its outbox was full", ["plant"], registry=r
        )
        self.dq_failed = Gauge("telemetry_dq_checks_failed", "Failed data-quality checks in the latest run", ["dataset"], registry=r)
        self.device_staleness = Gauge("telemetry_device_staleness_seconds", "Seconds since the last reading per device", ["device"], registry=r)

    def observe_batch(self, res, lag: int, watermark: datetime | None, now: datetime | None = None) -> None:
        self.records.inc(res.records)
        self.silver.inc(res.to_silver)
        self.duplicates.inc(res.duplicates)
        self.late.inc(res.late)
        self.out_of_range.inc(res.out_of_range)
        for reason, n in res.reasons.items():
            self.dead.labels(reason).inc(n)
        self.lag.set(lag)
        if watermark:
            self.watermark_delay.set(((now or datetime.now(UTC)) - watermark).total_seconds())
        self.batch_seconds.observe(res.seconds)

    def observe_gateway(self, plant: str, buffered: int, dropped: int) -> None:
        self.gateway_buffered.labels(plant).set(buffered)
        self.gateway_dropped.labels(plant).set(dropped)

    def observe_dq(self, results) -> None:
        failed: dict[str, int] = {}
        for r in results:
            failed.setdefault(r.dataset, 0)
            failed[r.dataset] += not r.success
        for ds, n in failed.items():
            self.dq_failed.labels(ds).set(n)
