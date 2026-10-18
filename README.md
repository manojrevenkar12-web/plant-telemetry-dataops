# plant-telemetry-dataops

Factory-floor data for AI. Plant sensors send readings over MQTT to an edge gateway, which checks them against data contracts, pseudonymises operator badges and buffers everything locally when the network is down. From there the data goes through Kafka (or Azure Event Hubs) into Delta Lake bronze, silver and gold tables. Batch jobs backfill historian data and reconcile shift counts against the MES. Every job runs data-quality checks (Great Expectations) and emits OpenLineage events, and a REST + GraphQL API serves the data and an anomaly model.

> Work in progress: the README, Terraform, Docker and CI are still being added.

```bash
pip install -e ".[dev]"
telemetry demo          # simulated 2-hour shift across 3 plants, about a minute
pytest
```
