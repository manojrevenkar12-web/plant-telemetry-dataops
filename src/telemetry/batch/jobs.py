"""Batch jobs that complement the stream.

* `backfill_historian` — the plant historian's CSV export (one-minute averages, wide format) is
  reshaped to long format and merged into silver/historian_1m and gold. Re-running the same file
  changes nothing (merge on device, metric, minute). Used for history the stream never saw and
  to train models.
* `reconcile_shift` — compares packs counted by line sensors with what the MES reports for the
  shift. Differences above 2% are flagged: either a sensor is miscounting or the MES has a
  booking error — both corrupt OEE and any AI built on it.

Why both batch and streaming: the stream gives seconds-level freshness for monitoring and
alerting; batch handles large historical loads, reconciliation against systems of record and
anything that needs a complete, closed period.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ..lake.tables import Lake

RECONCILIATION_TOLERANCE = 0.02


def write_historian_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


def backfill_historian(lake: Lake, csv_path: Path) -> int:
    wide = pl.read_csv(csv_path, try_parse_dates=False)
    tags = [c for c in wide.columns if c != "timestamp"]
    long = (
        wide.unpivot(index="timestamp", on=tags, variable_name="tag", value_name="value")
        .with_columns(
            pl.col("tag").str.split(".").list.get(0).alias("device_id"),
            pl.col("tag").str.split(".").list.get(1).alias("metric"),
            pl.col("timestamp").str.to_datetime(time_zone="UTC").dt.cast_time_unit("us").alias("minute"),
        )
        .with_columns(
            pl.col("device_id").str.slice(0, 4).alias("plant_id"),
            pl.col("device_id").str.slice(5, 2).alias("line_id"),
            pl.col("minute").dt.date().alias("event_date"),
            pl.lit(csv_path.name).alias("source_file"),
        )
        .drop_nulls("value")
        .select(["device_id", "plant_id", "line_id", "metric", "minute", "value", "event_date", "source_file"])
    )
    lake.merge("silver/historian_1m", long.to_arrow())
    gold = long.with_columns(
        pl.lit(1, dtype=pl.Int64).alias("n"),
        pl.col("value").alias("avg"),
        pl.col("value").alias("min"),
        pl.col("value").alias("max"),
        pl.col("value").alias("sum"),
        pl.lit(0, dtype=pl.Int64).alias("bad_readings"),
        pl.lit(0, dtype=pl.Int64).alias("late_readings"),
        pl.lit("historian").alias("source"),
    )
    lake.merge("gold/metrics_1m", gold.to_arrow())
    return long.height


def reconcile_shift(lake: Lake, mes_report: list[dict], shift_start: datetime, minutes: int) -> pl.DataFrame:
    end = shift_start.timestamp() + minutes * 60
    gold = lake.read("gold/metrics_1m", filters=[("metric", "=", "good_count"), ("source", "=", "stream")])
    sensor = (
        gold.filter((pl.col("minute") >= shift_start) & (pl.col("minute") < datetime.fromtimestamp(end, UTC)))
        .group_by(["plant_id", "line_id"])
        .agg(pl.col("sum").sum().alias("sensor_good_packs"))
    )
    mes = pl.DataFrame(mes_report).select(["plant_id", "line_id", pl.col("reported_good_packs").cast(pl.Float64).alias("mes_good_packs")])
    out = (
        mes.join(sensor, on=["plant_id", "line_id"], how="left")
        .with_columns(((pl.col("mes_good_packs") - pl.col("sensor_good_packs")) / pl.col("sensor_good_packs")).alias("difference_pct"))
        .with_columns(
            pl.when(pl.col("sensor_good_packs").is_null())
            .then(pl.lit("no_sensor_data"))
            .when(pl.col("difference_pct").abs() > RECONCILIATION_TOLERANCE)
            .then(pl.lit("mismatch"))
            .otherwise(pl.lit("ok"))
            .alias("status"),
            pl.lit(shift_start).dt.cast_time_unit("us").dt.replace_time_zone("UTC").alias("shift_start"),
            pl.lit(datetime.now(UTC)).dt.cast_time_unit("us").alias("checked_at"),
        )
    )
    lake.merge("gold/shift_reconciliation", out.to_arrow())
    return out.sort(["plant_id", "line_id"])
