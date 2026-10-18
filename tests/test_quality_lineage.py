import json
from datetime import timedelta

import polars as pl
import pytest

from telemetry.edge import devices
from telemetry.lineage.openlineage import lineage
from telemetry.quality import checks

T0 = devices.SHIFT_START


def _silver(n=50, **override):
    rows = [
        {
            "message_id": f"m{i}",
            "device_id": "PL01-L1-VIB-01",
            "plant_id": "PL01",
            "line_id": "L1",
            "metric": "vibration_mm_s",
            "seq": i,
            "event_time": T0 + timedelta(seconds=10 * i),
            "in_range": True,
            "arrival_delay_s": 1.0,
            "operator_ref": "op_0123456789abcdef",
        }
        for i in range(n)
    ]
    rows[0] |= override
    return pl.DataFrame(rows)


def test_clean_data_passes_silver_suite():
    assert all(r.success for r in checks.check_silver(_silver()))


def test_raw_badge_number_in_silver_fails_the_privacy_check():
    failed = [r.check for r in checks.check_silver(_silver(operator_ref="B-100017")) if not r.success]
    assert any("operator_ref" in c for c in failed)


def test_duplicate_natural_key_fails_uniqueness():
    df = pl.concat([_silver(5), _silver(5)])
    failed = [r.check for r in checks.check_silver(df) if not r.success]
    assert any("unique" in c for c in failed)


def test_freshness_flags_silent_devices():
    res = {r.check: r.success for r in checks.freshness(_silver(), T0 + timedelta(minutes=9))}
    assert res["freshness(PL01-L1-VIB-01)"] is True
    assert res["freshness(DE01-L1-VIB-01)"] is False


def test_lineage_emits_start_and_complete_with_facets(tmp_path):
    log = tmp_path / "events.jsonl"
    with lineage("job.a", ["silver/readings"], ["gold/metrics_1m"], log) as run:
        run.rows("gold/metrics_1m", 42)
        run.assertion("gold/metrics_1m", "unique_key", True)
    events = [json.loads(line) for line in log.read_text().splitlines()]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    out = events[1]["outputs"][0]
    assert out["outputFacets"]["outputStatistics"]["rowCount"] == 42
    assert out["facets"]["dataQualityAssertions"]["assertions"][0]["assertion"] == "unique_key"
    assert {f["name"] for f in out["facets"]["schema"]["fields"]} >= {"device_id", "minute", "avg"}
    assert events[0]["run"]["runId"] == events[1]["run"]["runId"]


def test_lineage_emits_fail_with_error(tmp_path):
    log = tmp_path / "events.jsonl"
    with pytest.raises(ValueError), lineage("job.b", [], ["gold/metrics_1m"], log):
        raise ValueError("disk full")
    last = json.loads(log.read_text().splitlines()[-1])
    assert last["eventType"] == "FAIL" and "disk full" in last["run"]["facets"]["errorMessage"]["message"]
