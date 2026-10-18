"""Command line: `telemetry demo | serve | contracts-check | process`."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _demo(a) -> int:
    from . import demo

    report = demo.run(Path(a.out), minutes=a.minutes, kafka=a.kafka or bool(os.environ.get("KAFKA_BOOTSTRAP")), historian_days=a.historian_days)
    s = report["stream"]
    print("\nResult")
    print(f"  devices emitted          {report['emitted_by_devices']:>9,}")
    print(f"  silver readings          {report['silver_rows']:>9,}  (unique keys {report['silver_unique_keys']:,})")
    print(f"  duplicates dropped       {s['duplicates']:>9,}")
    print(f"  dead letters             {s['dead_letters']:>9,}  {s['dead_letter_reasons']}")
    print(f"  late readings            {s['late']:>9,}")
    print(f"  outage buffer peak       {report['outage']['peak_buffered']:>9,}  lost {report['outage']['lost']}")
    print(f"  raw badges in silver     {report['raw_badges_in_silver']:>9}")
    print(f"  historian rows           {report['historian']['rows']:>9,}  (after re-run {report['historian']['rows_after_rerun']:,})")
    for r in report["reconciliation"]:
        print(f"  reconcile {r['plant_id']}-{r['line_id']}         {r['status']:>9}  {r['difference_pct'] * 100:+.2f}%")
    print(f"  DQ checks                {report['dq']['checks']:>9}  failed: {[f['check'] for f in report['dq']['failed']]}")
    print(f"  stale at minute 50       {report['dq_mid_shift_stale_devices']}")
    print(f"  anomaly model            {json.dumps(report['model']['evaluation'])}")
    print(f"  lineage events           {report['lineage_events']:>9}")
    print(f"  total                    {report['total_seconds']:>8}s   → {a.out}/report.json")
    return 0


def _serve(a) -> int:
    import uvicorn

    os.environ.setdefault("LAKE_URI", str(Path(a.data) / "lake"))
    os.environ.setdefault("MODEL_REGISTRY", str(Path(a.data) / "models"))
    os.environ.setdefault("LINEAGE_LOG", str(Path(a.data) / "lineage" / "events.jsonl"))
    uvicorn.run("telemetry.api.app:create_app", factory=True, host=a.host, port=a.port, log_level="warning")
    return 0


def _contracts(a) -> int:
    from .contracts import registry

    old, new = json.loads(Path(a.old).read_text()), json.loads(Path(a.new).read_text())
    problems = registry.check_compatibility(old, new)
    for p in problems:
        print(f"BREAKING: {p}")
    print("compatible" if not problems else f"{len(problems)} breaking change(s)")
    return 1 if problems else 0


def _process(a) -> int:
    """Run the stream processor against Kafka / Event Hubs until interrupted."""
    from prometheus_client import start_http_server

    from .bus.base import TOPIC_DLQ, TOPIC_TELEMETRY
    from .bus.kafka import KafkaConsumer
    from .lake.tables import Lake
    from .observability import PipelineMetrics
    from .stream.processor import StreamProcessor

    metrics = PipelineMetrics()
    start_http_server(a.metrics_port, registry=metrics.registry)
    proc = StreamProcessor(KafkaConsumer(a.group, [TOPIC_TELEMETRY, TOPIC_DLQ]), Lake(), metrics)
    print(f"processing; metrics on :{a.metrics_port}", flush=True)
    while True:
        res = proc.run_once()
        if res.records:
            print(
                json.dumps({"records": res.records, "silver": res.to_silver, "dlq": res.dead_letters, "late": res.late, "s": res.seconds}), flush=True
            )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="telemetry")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo", help="simulate a shift end to end")
    d.add_argument("--out", default="demo-output")
    d.add_argument("--minutes", type=int, default=120)
    d.add_argument("--historian-days", type=int, default=7)
    d.add_argument("--kafka", action="store_true", help="use Kafka/Event Hubs (KAFKA_BOOTSTRAP) instead of the in-memory bus")
    d.set_defaults(fn=_demo)
    s = sub.add_parser("serve", help="REST + GraphQL API over the lake")
    s.add_argument("--data", default="demo-output")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.set_defaults(fn=_serve)
    c = sub.add_parser("contracts-check", help="fail if a new schema version breaks consumers of the old one")
    c.add_argument("old")
    c.add_argument("new")
    c.set_defaults(fn=_contracts)
    pr = sub.add_parser("process", help="run the stream processor against Kafka")
    pr.add_argument("--group", default="stream-processor")
    pr.add_argument("--metrics-port", type=int, default=9108)
    pr.set_defaults(fn=_process)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
