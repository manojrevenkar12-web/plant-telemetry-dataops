# plant-telemetry-dataops

Factory-floor data, made ready for AI. Line sensors in three snack plants send readings over MQTT to an edge gateway in each plant. The gateway checks every message against a data contract, upgrades legacy firmware messages, pseudonymises operator badge numbers and buffers to disk when the plant's network link drops. From there the data goes through Kafka (Azure Event Hubs in the cloud) into Delta Lake bronze, silver and gold tables, with batch jobs alongside for historian backfill and MES reconciliation. Every job runs data-quality checks and reports lineage, and a REST + GraphQL API serves the curated data and a vibration-anomaly model to engineers, apps and AI agents.

```
 line devices ──MQTT──▶ edge gateway (per plant) ──Kafka / Event Hubs──▶ stream processor ──▶ Delta Lake
 (v1 + v2 firmware)     contracts · upcast v1→v2                           micro-batches          bronze  raw + bus offsets
                        GDPR pseudonymisation                              dedup · watermark      silver  validated readings
                        clock-skew check                                   late-data handling     gold    1-minute aggregates
                        SQLite store-and-forward                           dead letters                    shift reconciliation
                                                                                                    ops     data-quality results
 historian CSV ──────────────────────────▶ batch backfill ──────────────────────────────────────────▶ silver/historian_1m
 MES shift report ───────────────────────▶ reconciliation ──────────────────────────────────────────▶ gold/shift_reconciliation

 every job ──▶ OpenLineage events (file, Marquez or Purview)    Great Expectations + freshness/completeness ──▶ ops/dq_results
 FastAPI REST v1 + GraphQL (plant-scoped API keys, row limits, audit log)   Prometheus metrics + alert rules   Terraform for Azure
```

## Run it

```bash
pip install -e ".[dev]"
telemetry demo                  # one simulated 2-hour shift, 3 plants, 45 devices; about a minute
telemetry serve                 # API on :8000 over the demo lake; docs at /docs, GraphQL at /graphql
pytest                          # 47 tests; the Kafka one runs in CI against a real broker
docker compose up --build       # the same demo through a real Kafka broker, plus API and Prometheus
```

```bash
curl -H "X-API-Key: pl01-demo-key" localhost:8000/v1/lines/PL01/L2/health
curl -H "X-API-Key: pl01-demo-key" -d '{"query":"{ reconciliation { lineId status differencePct } }"}' -H 'content-type: application/json' localhost:8000/graphql
curl -H "X-API-Key: ops-demo-key" localhost:8000/v1/pipeline/status
```

## What the demo shift contains, and what the pipeline does with it

The simulator puts the problems real plants have into a two-hour shift on purpose, and the tests check that each one is handled. The figures are from `telemetry demo` (seed 7); every number is reproducible.

| Planted in the shift | What happens | Result |
|---|---|---|
| 38,742 messages from 45 devices | contracts, dedup, bronze → silver → gold | 38,340 readings in silver, each exactly once (emitted = silver + duplicates + dead letters) |
| 1% MQTT QoS-1 redeliveries | silver MERGE on (device, metric, seq) | 392 duplicates dropped |
| Plant DE01 on old firmware (°F, epoch-ms, no units) | gateway upcasts contract v1 → v2 | 13,087 messages converted; fryer temperatures land in °C |
| Operator badge numbers on counter messages | HMAC pseudonym with a per-plant key, at the edge | 0 raw badge numbers in bronze or silver (also a Great Expectations check) |
| Truncated JSON and an unknown metric | dead letters with a reason | 10 dead letters: 5 `malformed_json`, 5 `contract_v2` |
| PL01 loses its network link for 25 minutes | SQLite outbox buffers, replays on recovery | 2,725 messages buffered, 0 lost; 1,183 readings arrive later than the 15-minute lateness bound, are flagged late and still correct their minute aggregates |
| A sensor clock running 7 minutes fast | skew measured at the gateway; the watermark ignores untrusted clocks | 724 readings flagged; other devices are not wrongly marked late |
| A faulty temperature sensor sending 9999 | kept in silver, flagged out-of-range, excluded from aggregates | gold averages unaffected |
| Line NL01-L1 offline for 15 minutes | freshness and completeness checks | all 5 devices stale at minute 50; completeness 87.5% → the only failing DQ check |
| The MES over-reports PL01-L1 by 8% | sensor pack counts vs MES report, 2% tolerance | only PL01-L1 flagged `mismatch`, +8.00% |
| A gearbox fault building up on PL01-L2 | robust z-score model trained on 7 days of historian data | first alert 6 minutes after onset, recall 0.88, 0 false alarms in 1,015 healthy device-minutes |

Also measured: 7 days of historian export (362,880 rows) backfill in about 3 seconds, and running the backfill again leaves the row count unchanged. Compaction takes silver from 120 micro-batch files to 1. The run writes 488 OpenLineage events, every run a START/COMPLETE pair.

## Design decisions

**Delivery guarantees.** The bus is at-least-once; the lake is effectively exactly-once. Bronze keeps every record with its topic, partition and offset, so any table can be rebuilt. Silver is an insert-only MERGE on the natural key, so redeliveries and gateway re-sends change nothing. Gold recomputes every minute window a batch touched from silver, so a late reading corrects its window instead of adding a second one. Offsets are committed only after all writes succeed. A test crashes the processor between write and commit and checks that the replay changes no table.

**Event time, not arrival time.** Aggregates use the device's timestamp. A watermark (latest trusted event time minus 15 minutes) marks late data. Late readings are stored and counted, never dropped, because a plant outage is exactly when maintenance needs the data.

**Batch and streaming side by side.** Streaming gives seconds-level freshness for alerts and line-health APIs. Batch handles large historical loads (the historian), closed periods (a shift) and reconciliation against systems of record (MES). They write to the same tables with the same merge keys, separated by a `source` column.

**Contracts at the edge.** JSON Schema contracts, versioned. `telemetry contracts-check old.json new.json` fails a pull request that would break consumers: removed fields, new required fields, narrowed enums, changed types. Bad data is rejected as close to the source as possible and dead-lettered with a reason, never silently dropped.

**Security and compliance.**
- Personal data (operator badges) is pseudonymised before it leaves the plant (GDPR data minimisation). The key never leaves the plant gateway and Key Vault.
- API keys are stored hashed. Each key has a role and the plants it may see; other plants' devices return 404, in REST and GraphQL alike.
- Bulk extraction is limited: row caps, a 24-hour window on raw data, a rate limit per key, and an audit log line for every data access.
- In Azure: storage accepts Entra ID only (account keys disabled), a managed identity is the only principal with data access, Event Hubs has send-only and listen-only rules, and Terraform rejects non-EU regions.
- The anomaly model is explainable (a per-device median and spread), and its model card records the Delta table version it was trained on, which supports EU AI Act documentation and traceability.

## Azure and Microsoft Fabric mapping

| Here | Azure / Fabric |
|---|---|
| Kafka client (`confluent-kafka`) | Event Hubs Kafka endpoint, SASL_SSL (Terraform included) |
| Delta tables via delta-rs (`LAKE_URI`) | ADLS Gen2 `abfss://` (Terraform included); OneLake shortcuts or Fabric Lakehouse read the same Delta tables |
| stream processor (Python micro-batches) | the same container on Container Apps (Terraform included); Databricks Structured Streaming or Fabric Eventstream at higher volume |
| batch jobs | Azure Data Factory / Fabric pipelines triggering the same jobs |
| OpenLineage events | Microsoft Purview (via an OpenLineage collector) or Marquez |
| Prometheus metrics and alerts | Azure Monitor managed Prometheus; Log Analytics for platform logs (Terraform included) |
| model card JSON registry | MLflow / Azure ML model registry |

## Honest limits

- The data is simulated. The failure modes are modelled on real plant problems, but no real OT system (OPC UA server, historian, MES) was connected.
- Kafka is exercised in CI against a real Apache Kafka broker: an integration test, plus the full shift through Kafka with an exactly-once check (about 17 minutes, because every micro-batch waits out a 1-second poll; not tuned). It was not run against Azure Event Hubs.
- The Terraform passes `fmt` and `validate` in CI and has not been applied to a subscription. Two things would need checking in a first deployment: delta-rs managed-identity auth inside Container Apps, and moving the model registry from a local folder to ADLS or MLflow.
- The processor is single-threaded Python. That is enough for this demo (about 650 readings per second end to end, simulator included). A plant network with many thousands of tags per second would use Spark Structured Streaming or Flink with the same contracts, keys and table design.
- The gateway outbox commits every message with SQLite WAL and `synchronous=NORMAL`: it survives a gateway crash or restart (tested), but a power loss can drop the last few commits.

## Layout

```
src/telemetry/
  contracts/   JSON Schema v1/v2, upcasting, compatibility rules
  edge/        device simulator, MQTT transport, gateway (validation, pseudonyms, store-and-forward)
  bus/         Kafka/Event Hubs, in-memory bus, plant link
  stream/      micro-batch processor (bronze → silver → gold)
  batch/       historian backfill, MES reconciliation
  lake/        Delta table schemas, merge keys, maintenance
  quality/     Great Expectations suites, freshness, completeness
  lineage/     OpenLineage events
  ml/          vibration anomaly model and evaluation
  api/         FastAPI REST v1 + Strawberry GraphQL
infra/terraform/   Event Hubs, ADLS Gen2, Key Vault, managed identity, Container Apps, Log Analytics
ops/prometheus/    scrape config and alert rules
```

MIT licence. Built by Manoj Kumar Prakash.
