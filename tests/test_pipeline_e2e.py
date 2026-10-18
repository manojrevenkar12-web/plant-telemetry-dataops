"""End to end: one simulated shift through edge, bus, stream, batch, quality, lineage and model."""

import json

import polars as pl

from telemetry.lake.tables import Lake


def test_every_valid_reading_lands_exactly_once(demo_run):
    _, r = demo_run
    s = r["stream"]
    assert r["silver_rows"] == r["silver_unique_keys"]
    assert r["emitted_by_devices"] == r["silver_rows"] + s["duplicates"] + s["dead_letters"]
    assert s["dead_letter_reasons"] == {"malformed_json": 5, "contract_v2": 5}


def test_outage_is_buffered_replayed_and_marked_late(demo_run):
    _, r = demo_run
    assert r["outage"]["peak_buffered"] > 2500 and r["outage"]["lost"] == 0
    assert r["stream"]["late"] > 0
    assert r["gateways"]["PL01"]["forward_failures"] > 0


def test_legacy_firmware_is_upcast_and_no_raw_badges_reach_the_cloud(demo_run):
    out, r = demo_run
    assert r["gateways"]["DE01"]["upcast_v1"] > 10_000
    assert r["raw_badges_in_silver"] == 0
    bronze = Lake(out / "lake").read("bronze/telemetry_raw", columns=["value"])
    assert not bronze["value"].str.contains(r"B-\d{6}").any()
    temps = Lake(out / "lake").read("silver/readings", filters=[("plant_id", "=", "DE01"), ("metric", "=", "fryer_temp_c")])
    assert temps["value"].is_between(160, 190).all()  # Celsius, not Fahrenheit


def test_gold_matches_silver(demo_run):
    out, _ = demo_run
    lake = Lake(out / "lake")
    silver = lake.read("silver/readings").filter(pl.col("in_range"))
    expect = (
        silver.with_columns(pl.col("event_time").dt.truncate("1m").alias("minute"))
        .group_by(["device_id", "metric", "minute"])
        .agg(pl.col("value").sum().alias("expected"))
    )
    gold = lake.read("gold/metrics_1m", filters=[("source", "=", "stream")])
    joined = gold.join(expect, on=["device_id", "metric", "minute"], how="inner")
    assert joined.height == expect.height
    assert (joined["sum"] - joined["expected"]).abs().max() < 1e-6


def test_reconciliation_catches_only_the_mes_booking_error(demo_run):
    _, r = demo_run
    flagged = {(x["plant_id"], x["line_id"]): x for x in r["reconciliation"] if x["status"] != "ok"}
    assert list(flagged) == [("PL01", "L1")]
    assert abs(flagged[("PL01", "L1")]["difference_pct"] - 0.08) < 0.001


def test_quality_finds_the_offline_line(demo_run):
    _, r = demo_run
    assert [f["check"] for f in r["dq"]["failed"]] == ["completeness(NL01-L1)"]
    assert len(r["dq_mid_shift_stale_devices"]) == 5 and all("NL01-L1" in c for c in r["dq_mid_shift_stale_devices"])


def test_historian_backfill_is_idempotent(demo_run):
    _, r = demo_run
    assert r["historian"]["rows"] == r["historian"]["rows_after_rerun"] == 2 * 24 * 60 * 36


def test_model_detects_the_gearbox_fault_without_false_alarms(demo_run):
    _, r = demo_run
    ev = r["model"]["evaluation"]
    assert ev["false_alarms"] == 0 and ev["recall"] >= 0.8 and ev["first_alert_minutes_after_onset"] <= 10
    assert r["model"]["trained_on"]["delta_version"] is not None


def test_every_job_has_complete_lineage(demo_run):
    out, _ = demo_run
    events = [json.loads(line) for line in (out / "lineage" / "events.jsonl").read_text().splitlines()]
    runs: dict[str, list[str]] = {}
    for e in events:
        runs.setdefault(e["run"]["runId"], []).append(e["eventType"])
    assert all(states == ["START", "COMPLETE"] for states in runs.values())
    jobs = {e["job"]["name"] for e in events}
    assert jobs >= {
        "stream.bronze_silver_gold",
        "batch.historian_backfill",
        "batch.shift_reconciliation",
        "quality.checks",
        "ml.train_vibration_anomaly",
    }
    dq = [e for e in events if e["job"]["name"] == "quality.checks" and e["eventType"] == "COMPLETE"][0]
    assertions = [a for o in dq["outputs"] for a in o["facets"].get("dataQualityAssertions", {}).get("assertions", [])]
    assert assertions and any(not a["success"] for a in assertions)


def test_maintenance_compacts_micro_batch_files(demo_run):
    _, r = demo_run
    assert r["maintenance"]["silver_files_before"] > 50 and r["maintenance"]["silver_files_after"] <= 2
