"""End-to-end run of one simulated two-hour shift across three plants.

devices → per-plant edge gateway (contracts, pseudonymisation, store-and-forward)
        → bus (in-memory, or Kafka / Event Hubs when KAFKA_BOOTSTRAP is set)
        → stream processor (bronze → silver → gold, every simulated minute)
then the batch side: historian backfill, MES shift reconciliation, data-quality checks,
model training and evaluation, table maintenance — every job emitting OpenLineage events.

The clock is simulated, so a two-hour shift runs in about a minute and every number in the
report is reproducible from the seed.
"""

from __future__ import annotations

import json
import shutil
import time
from collections import defaultdict
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

from .batch import jobs
from .bus.base import TOPIC_DLQ, TOPIC_TELEMETRY
from .bus.link import Link
from .bus.memory import MemoryBus
from .edge import devices
from .edge.gateway import EdgeGateway
from .lake.tables import Lake
from .lineage.openlineage import lineage
from .ml import anomaly
from .observability import PipelineMetrics
from .quality import checks
from .stream.processor import StreamProcessor

MES_ERROR = ("PL01", "L1", 1.08)  # the MES over-reports this line by 8% (a booking error to be caught)


def _bus(kafka: bool, run_id: str):
    if kafka:
        from .bus import kafka as k

        k.ensure_topics([TOPIC_TELEMETRY, TOPIC_DLQ])
        return k.KafkaProducer(), lambda: k.KafkaConsumer(f"stream-processor-{run_id}", [TOPIC_TELEMETRY, TOPIC_DLQ]), None
    bus = MemoryBus()
    return bus.producer(), lambda: bus.consumer("stream-processor", [TOPIC_TELEMETRY, TOPIC_DLQ]), bus


def run(out: Path = Path("demo-output"), minutes: int = 120, kafka: bool = False, historian_days: int = 7, log=print) -> dict:
    t_start = time.perf_counter()
    if out.exists():
        shutil.rmtree(out)
    (out / "outbox").mkdir(parents=True)
    lake = Lake(out / "lake")
    lineage_log = out / "lineage" / "events.jsonl"
    registry_dir = out / "models"
    metrics = PipelineMetrics()
    run_id = str(int(time.time()))

    producer, make_consumer, _ = _bus(kafka, run_id)
    links = {p: Link(producer) for p in devices.PLANTS}
    gateways = {p: EdgeGateway(p, links[p], out / "outbox" / f"{p}.db", pseudonym_key=f"key-{p}".encode()) for p in devices.PLANTS}
    processor = StreamProcessor(make_consumer(), lake, metrics)

    totals: dict[str, int] = defaultdict(int)
    reasons: dict[str, int] = defaultdict(int)
    true_good: dict[tuple[str, str], float] = defaultdict(float)
    seen_ids: set[bytes] = set()
    outage_peak = 0
    mid_shift_dq = None
    outage_plant, outage_from, outage_to = devices.NETWORK_OUTAGE

    def process_minute(now):
        nonlocal outage_peak
        for p, gw in gateways.items():
            gw.forward()
            metrics.observe_gateway(p, gw.buffered(), gw.stats.dropped_overflow)
        outage_peak = max(outage_peak, gateways[outage_plant].buffered())
        while True:
            with lineage(
                "stream.bronze_silver_gold",
                [TOPIC_TELEMETRY, TOPIC_DLQ],
                ["bronze/telemetry_raw", "bronze/dead_letters", "silver/readings", "gold/metrics_1m"],
                lineage_log,
            ) as lr:
                res = processor.run_once(now=now)
                lr.rows("bronze/telemetry_raw", res.records)
                lr.rows("silver/readings", res.to_silver)
                lr.rows("bronze/dead_letters", res.dead_letters)
                lr.rows("gold/metrics_1m", res.gold_windows)
            if not res.records:
                break
            for k in ("records", "to_silver", "duplicates", "dead_letters", "late", "out_of_range", "gold_windows"):
                totals[k] += getattr(res, k)
            for r, n in res.reasons.items():
                reasons[r] += n

    log(f"Simulating a {minutes}-minute shift at 3 plants ({len(devices.device_ids())} devices)…")
    current_minute = 0
    for em in devices.simulate(minutes):
        m = int((em.sent_at - devices.SHIFT_START).total_seconds() // 60)
        while m > current_minute:  # minute boundary: forward, process, maybe toggle the outage
            now = devices.SHIFT_START + timedelta(minutes=current_minute + 1)
            process_minute(now)
            current_minute += 1
            links[outage_plant].up = not (outage_from <= current_minute < outage_to)
            if current_minute == outage_from:
                log(f"  minute {current_minute}: {outage_plant} loses its network link — gateway buffers locally")
            if current_minute == outage_to:
                log(f"  minute {current_minute}: {outage_plant} link restored — replaying {gateways[outage_plant].buffered():,} buffered messages")
            if current_minute == 50:
                silver = lake.read("silver/readings")
                mid_shift_dq = [r for r in checks.freshness(silver, now) if not r.success]
        totals["emitted"] += 1
        # ground truth for the MES report: every distinct good-count message the devices sent
        if b"good" in em.payload and em.payload not in seen_ids:
            seen_ids.add(em.payload)
            msg = json.loads(em.payload)
            if msg.get("metric") == "good_count" or msg.get("tag") == "good":
                line = msg.get("line_id") or msg["dev"][5:7]
                true_good[(em.plant_id, line)] += float(msg.get("value", msg.get("val", 0)))
        gateways[em.plant_id].handle(em.payload, em.sent_at)
    links[outage_plant].up = True
    end = devices.SHIFT_START + timedelta(minutes=minutes)
    process_minute(end + timedelta(minutes=1))
    stream_seconds = round(time.perf_counter() - t_start, 1)
    log(
        f"  stream done in {stream_seconds}s: {totals['to_silver']:,} readings in silver, {totals['duplicates']:,} duplicates dropped, "
        f"{totals['dead_letters']} dead letters, {totals['late']:,} late"
    )

    # ---- batch: historian backfill
    log(f"Backfilling {historian_days} days of historian data…")
    t0 = time.perf_counter()
    csv_path = jobs.write_historian_csv(devices.historian_export(historian_days), out / "landing" / "historian_export.csv")
    with lineage("batch.historian_backfill", [f"file://{csv_path.name}"], ["silver/historian_1m", "gold/metrics_1m"], lineage_log) as lr:
        hist_rows = jobs.backfill_historian(lake, csv_path)
        lr.rows("silver/historian_1m", hist_rows)
    rerun = jobs.backfill_historian(lake, csv_path)  # idempotency proof: re-running changes nothing
    hist_count = lake.table("silver/historian_1m").to_pyarrow_dataset().count_rows()
    backfill_seconds = round(time.perf_counter() - t0, 1)

    # ---- batch: MES reconciliation
    mes = [
        {"plant_id": p, "line_id": ln, "reported_good_packs": round(v * (MES_ERROR[2] if (p, ln) == MES_ERROR[:2] else 1.0))}
        for (p, ln), v in sorted(true_good.items())
    ]
    with lineage("batch.shift_reconciliation", ["gold/metrics_1m", "file://mes_shift_report.json"], ["gold/shift_reconciliation"], lineage_log) as lr:
        recon = jobs.reconcile_shift(lake, mes, devices.SHIFT_START, minutes)
        lr.rows("gold/shift_reconciliation", recon.height)

    # ---- data quality
    with lineage(
        "quality.checks", ["silver/readings", "gold/metrics_1m"], ["ops/dq_results", "silver/readings", "gold/metrics_1m"], lineage_log
    ) as lr:
        dq = checks.run_checks(lake, end + timedelta(minutes=1), window=(devices.SHIFT_START, end))
        for r in dq:
            if not r.check.startswith("freshness"):
                lr.assertion("silver/readings" if r.dataset == "silver_readings" else "gold/metrics_1m", r.check, r.success)
        lr.rows("ops/dq_results", len(dq))
    metrics.observe_dq(dq)

    # ---- model
    with lineage("ml.train_vibration_anomaly", ["silver/historian_1m"], [], lineage_log):
        card = anomaly.train(lake, registry_dir)
    fault_plant, fault_line, fault_minute = devices.GEARBOX_FAULT
    evaluation = anomaly.evaluate(card, lake, f"{fault_plant}-{fault_line}-VIB-01", devices.SHIFT_START + timedelta(minutes=fault_minute))

    # ---- maintenance
    files_before = len(lake.table("silver/readings").file_uris())
    for t in ("silver/readings", "gold/metrics_1m", "bronze/telemetry_raw"):
        lake.maintain(t)
    files_after = len(lake.table("silver/readings").file_uris())

    silver = lake.read("silver/readings")
    report = {
        "shift_minutes": minutes,
        "bus": "kafka" if kafka else "memory",
        "emitted_by_devices": totals["emitted"],
        "gateways": {p: asdict(g.stats) for p, g in gateways.items()},
        "stream": {
            **{k: totals[k] for k in ("records", "to_silver", "duplicates", "dead_letters", "late", "out_of_range", "gold_windows")},
            "dead_letter_reasons": dict(reasons),
            "seconds": stream_seconds,
        },
        "silver_rows": silver.height,
        "silver_unique_keys": silver.select(["device_id", "metric", "seq"]).unique().height,
        "raw_badges_in_silver": int(silver["operator_ref"].drop_nulls().str.contains(r"B-\d").sum()),
        "outage": {
            "plant": outage_plant,
            "minutes": outage_to - outage_from,
            "peak_buffered": outage_peak,
            "lost": gateways[outage_plant].stats.dropped_overflow,
        },
        "historian": {"rows": hist_rows, "rows_after_rerun": hist_count, "rerun_rows_processed": rerun, "seconds": backfill_seconds},
        "reconciliation": recon.select(["plant_id", "line_id", "sensor_good_packs", "mes_good_packs", "difference_pct", "status"]).to_dicts(),
        "dq": {"checks": len(dq), "failed": [asdict(r) for r in dq if not r.success]},
        "dq_mid_shift_stale_devices": [r.check for r in (mid_shift_dq or [])],
        "model": {"version": card["version"], "trained_on": card["training_data"], "evaluation": evaluation},
        "maintenance": {"silver_files_before": files_before, "silver_files_after": files_after},
        "table_versions": {t: lake.version(t) for t in ("bronze/telemetry_raw", "silver/readings", "gold/metrics_1m", "silver/historian_1m")},
        "lineage_events": sum(1 for _ in lineage_log.open()) if lineage_log.exists() else 0,
        "total_seconds": round(time.perf_counter() - t_start, 1),
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str))
    (out / "metrics.prom").write_bytes(__import__("prometheus_client").generate_latest(metrics.registry))
    return report
