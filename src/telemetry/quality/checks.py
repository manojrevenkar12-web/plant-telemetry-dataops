"""Data-quality checks on silver and gold: Great Expectations suites plus freshness and completeness
monitors that only make sense for streaming data. Results are written to ops/dq_results and
attached to the OpenLineage run, so a failing check is visible next to the data it describes."""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl

from ..edge.devices import device_ids
from ..lake.tables import Lake

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
logging.getLogger("great_expectations").setLevel(logging.WARNING)

import great_expectations as gx  # noqa: E402

FRESHNESS_LIMIT = timedelta(minutes=5)
READINGS_PER_MINUTE = 6  # one reading every 10 s


@dataclass
class CheckResult:
    dataset: str
    check: str
    success: bool
    observed: str
    rows_checked: int


def _suite_results(name: str, df: pl.DataFrame, expectations: list) -> list[CheckResult]:
    ctx = gx.get_context(mode="ephemeral")
    try:
        ctx.variables.progress_bars = {"globally": False}
    except Exception:  # noqa: BLE001 - cosmetic setting, absent in some versions
        pass
    batch = ctx.data_sources.add_pandas(f"ds_{uuid.uuid4().hex[:6]}").add_dataframe_asset(name).add_batch_definition_whole_dataframe("all")
    suite = ctx.suites.add(gx.ExpectationSuite(name=f"{name}_suite"))
    for e in expectations:
        suite.add_expectation(e)
    vd = ctx.validation_definitions.add(gx.ValidationDefinition(name=f"{name}_v", data=batch, suite=suite))
    result = vd.run(batch_parameters={"dataframe": df.to_pandas()})
    out = []
    for r in result.results:
        cfg = r.expectation_config
        col = cfg.kwargs.get("column") or ",".join(cfg.kwargs.get("column_list", []))
        observed = r.result.get("unexpected_percent")
        out.append(
            CheckResult(
                name,
                f"{cfg.type}({col})",
                bool(r.success),
                f"unexpected {observed:.3f}%" if observed is not None else str(r.result.get("observed_value", "")),
                df.height,
            )
        )
    return out


def check_silver(df: pl.DataFrame) -> list[CheckResult]:
    E = gx.expectations
    return _suite_results(
        "silver_readings",
        df,
        [
            E.ExpectColumnValuesToNotBeNull(column="message_id"),
            E.ExpectCompoundColumnsToBeUnique(column_list=["device_id", "metric", "seq"]),
            E.ExpectColumnValuesToMatchRegex(column="device_id", regex=r"^[A-Z]{2}\d{2}-L\d-[A-Z]{3}-\d{2}$"),
            E.ExpectColumnValuesToBeInSet(
                column="metric", value_set=["fryer_temp_c", "vibration_mm_s", "line_speed_m_min", "power_kw", "good_count", "reject_count"]
            ),
            E.ExpectColumnValuesToBeInSet(column="in_range", value_set=[True], mostly=0.995),
            E.ExpectColumnValuesToBeBetween(column="arrival_delay_s", max_value=900, mostly=0.95),
            # Personal data control: only edge pseudonyms, never raw badge numbers.
            E.ExpectColumnValuesToNotMatchRegex(column="operator_ref", regex=r"B-\d{6}"),
            E.ExpectColumnValuesToMatchRegex(column="operator_ref", regex=r"^op_[0-9a-f]{16}$"),
        ],
    )


def check_gold(df: pl.DataFrame) -> list[CheckResult]:
    E = gx.expectations
    return _suite_results(
        "gold_metrics_1m",
        df,
        [
            E.ExpectCompoundColumnsToBeUnique(column_list=["device_id", "metric", "minute", "source"]),
            E.ExpectColumnValuesToBeBetween(column="n", min_value=1, max_value=READINGS_PER_MINUTE),
            E.ExpectColumnValuesToNotBeNull(column="avg", mostly=0.99),
        ],
    )


def freshness(silver: pl.DataFrame, now: datetime) -> list[CheckResult]:
    """Every known device must have sent data within the last 5 minutes."""
    last = silver.group_by("device_id").agg(pl.col("event_time").max().alias("last"))
    seen = dict(zip(last["device_id"].to_list(), last["last"].to_list(), strict=True))
    out = []
    for dev in device_ids():
        ts = seen.get(dev)
        age = (now - ts).total_seconds() / 60 if ts else None
        ok = age is not None and age <= FRESHNESS_LIMIT.total_seconds() / 60
        out.append(CheckResult("silver_readings", f"freshness({dev})", ok, f"last reading {age:.1f} min ago" if age is not None else "never", 0))
    return out


def completeness(silver: pl.DataFrame, start: datetime, end: datetime) -> list[CheckResult]:
    """Share of expected readings received per line over a window — gaps show offline devices."""
    window = silver.filter((pl.col("event_time") >= start) & (pl.col("event_time") < end))
    expected_per_device_metric = (end - start).total_seconds() / 60 * READINGS_PER_MINUTE
    got = window.group_by(["plant_id", "line_id"]).agg((pl.len() / (expected_per_device_metric * 6)).alias("ratio"))  # 6 metrics per line
    return [
        CheckResult(
            "silver_readings", f"completeness({r['plant_id']}-{r['line_id']})", r["ratio"] >= 0.95, f"{r['ratio'] * 100:.1f}% of expected readings", 0
        )
        for r in got.iter_rows(named=True)
    ]


def run_checks(lake: Lake, now: datetime, window: tuple[datetime, datetime] | None = None) -> list[CheckResult]:
    silver = lake.read("silver/readings")
    gold = lake.read("gold/metrics_1m")
    results = check_silver(silver) + check_gold(gold) + freshness(silver, now)
    if window:
        results += completeness(silver, *window)
    run_id = uuid.uuid4().hex[:12]
    lake.append(
        "ops/dq_results",
        [
            {
                "run_id": run_id,
                "dataset": r.dataset,
                "check": r.check,
                "success": r.success,
                "observed": r.observed,
                "rows_checked": r.rows_checked,
                "checked_at": datetime.now(UTC),
            }
            for r in results
        ],
    )
    return results
