"""Delta Lake tables (bronze / silver / gold) via delta-rs.

The lake URI is local in development and an `abfss://` OneLake or ADLS Gen2 path in Azure; delta-rs
takes the storage credentials from the environment (managed identity in Azure), so the
pipeline code does not change.

Every write that can be repeated (stream retries, batch re-runs) is a MERGE on a natural key,
so replaying the same input never creates duplicates.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

SCHEMAS: dict[str, pa.Schema] = {
    "bronze/telemetry_raw": pa.schema(
        [
            ("topic", pa.string()),
            ("partition", pa.int32()),
            ("offset", pa.int64()),
            ("key", pa.string()),
            ("value", pa.string()),
            ("contract", pa.string()),
            ("ingested_at", pa.timestamp("us", "UTC")),
            ("ingest_date", pa.date32()),
        ]
    ),
    "bronze/dead_letters": pa.schema(
        [
            ("source", pa.string()),
            ("reason", pa.string()),
            ("detail", pa.string()),
            ("payload", pa.string()),
            ("received_at", pa.timestamp("us", "UTC")),
        ]
    ),
    "silver/readings": pa.schema(
        [
            ("message_id", pa.string()),
            ("device_id", pa.string()),
            ("plant_id", pa.string()),
            ("line_id", pa.string()),
            ("metric", pa.string()),
            ("value", pa.float64()),
            ("unit", pa.string()),
            ("event_time", pa.timestamp("us", "UTC")),
            ("event_date", pa.date32()),
            ("seq", pa.int64()),
            ("quality", pa.string()),
            ("firmware", pa.string()),
            ("operator_ref", pa.string()),
            ("gateway_id", pa.string()),
            ("gateway_time", pa.timestamp("us", "UTC")),
            ("clock_skew_s", pa.float64()),
            ("arrival_delay_s", pa.float64()),
            ("is_late", pa.bool_()),
            ("in_range", pa.bool_()),
            ("processed_at", pa.timestamp("us", "UTC")),
        ]
    ),
    "silver/historian_1m": pa.schema(
        [
            ("device_id", pa.string()),
            ("plant_id", pa.string()),
            ("line_id", pa.string()),
            ("metric", pa.string()),
            ("minute", pa.timestamp("us", "UTC")),
            ("value", pa.float64()),
            ("event_date", pa.date32()),
            ("source_file", pa.string()),
        ]
    ),
    "gold/metrics_1m": pa.schema(
        [
            ("device_id", pa.string()),
            ("plant_id", pa.string()),
            ("line_id", pa.string()),
            ("metric", pa.string()),
            ("minute", pa.timestamp("us", "UTC")),
            ("n", pa.int64()),
            ("avg", pa.float64()),
            ("min", pa.float64()),
            ("max", pa.float64()),
            ("sum", pa.float64()),
            ("bad_readings", pa.int64()),
            ("late_readings", pa.int64()),
            ("source", pa.string()),
            ("event_date", pa.date32()),
        ]
    ),
    "gold/shift_reconciliation": pa.schema(
        [
            ("plant_id", pa.string()),
            ("line_id", pa.string()),
            ("shift_start", pa.timestamp("us", "UTC")),
            ("sensor_good_packs", pa.float64()),
            ("mes_good_packs", pa.float64()),
            ("difference_pct", pa.float64()),
            ("status", pa.string()),
            ("checked_at", pa.timestamp("us", "UTC")),
        ]
    ),
    "ops/dq_results": pa.schema(
        [
            ("run_id", pa.string()),
            ("dataset", pa.string()),
            ("check", pa.string()),
            ("success", pa.bool_()),
            ("observed", pa.string()),
            ("rows_checked", pa.int64()),
            ("checked_at", pa.timestamp("us", "UTC")),
        ]
    ),
}

PARTITIONS = {
    "bronze/telemetry_raw": ["ingest_date"],
    "silver/readings": ["event_date"],
    "silver/historian_1m": ["event_date"],
    "gold/metrics_1m": ["event_date"],
}
MERGE_KEYS = {
    "silver/readings": ["device_id", "metric", "seq"],
    "silver/historian_1m": ["device_id", "metric", "minute"],
    "gold/metrics_1m": ["device_id", "metric", "minute", "source"],
    "gold/shift_reconciliation": ["plant_id", "line_id", "shift_start"],
}


class Lake:
    def __init__(self, root: str | Path | None = None, storage_options: dict | None = None):
        self.root = str(root or os.environ.get("LAKE_URI", "lake"))
        self.storage_options = storage_options

    def uri(self, name: str) -> str:
        return f"{self.root.rstrip('/')}/{name}"

    def exists(self, name: str) -> bool:
        try:
            DeltaTable(self.uri(name), storage_options=self.storage_options)
            return True
        except TableNotFoundError:
            return False

    def table(self, name: str) -> DeltaTable:
        return DeltaTable(self.uri(name), storage_options=self.storage_options)

    def _arrow(self, name: str, rows: list[dict] | pa.Table) -> pa.Table:
        schema = SCHEMAS[name]
        if isinstance(rows, pa.Table):
            return rows.select(schema.names).cast(schema)
        return pa.Table.from_pylist(rows, schema=schema)

    def append(self, name: str, rows: list[dict] | pa.Table) -> int:
        t = self._arrow(name, rows)
        if t.num_rows:
            write_deltalake(self.uri(name), t, mode="append", partition_by=PARTITIONS.get(name), storage_options=self.storage_options)
        return t.num_rows

    def merge(self, name: str, rows: list[dict] | pa.Table, update: bool = True) -> dict:
        """Idempotent upsert on the table's natural key. update=False keeps the first version (dedup)."""
        t = self._arrow(name, rows)
        if not t.num_rows:
            return {"num_target_rows_inserted": 0, "num_target_rows_updated": 0}
        if not self.exists(name):
            write_deltalake(self.uri(name), t.slice(0, 0), mode="append", partition_by=PARTITIONS.get(name), storage_options=self.storage_options)
        keys = MERGE_KEYS[name]
        predicate = " and ".join(f"t.{k} = s.{k}" for k in keys)
        m = self.table(name).merge(t, predicate=predicate, source_alias="s", target_alias="t")
        if update:
            m = m.when_matched_update_all()
        return m.when_not_matched_insert_all().execute()

    def read(self, name: str, filters: list[tuple] | None = None, columns: list[str] | None = None) -> pl.DataFrame:
        """Read a table with (column, op, value) filters; partition filters prune files before reading."""
        if not self.exists(name):
            return pl.from_arrow(SCHEMAS[name].empty_table())
        lf = pl.scan_delta(self.uri(name), storage_options=self.storage_options)
        for col, op, value in filters or []:
            c = pl.col(col)
            lf = lf.filter(
                {"=": c == value, "!=": c != value, "in": c.is_in(list(value)), ">=": c >= value, ">": c > value, "<=": c <= value, "<": c < value}[
                    op
                ]
            )
        if columns:
            lf = lf.select(columns)
        return lf.collect()

    def version(self, name: str) -> int | None:
        return self.table(name).version() if self.exists(name) else None

    def maintain(self, name: str) -> None:
        """Compact small files written by micro-batches and remove unreferenced files (7-day retention)."""
        if self.exists(name):
            dt = self.table(name)
            dt.optimize.compact()
            dt.vacuum(retention_hours=168, enforce_retention_duration=True, dry_run=False)
