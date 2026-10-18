import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")

from telemetry import demo  # noqa: E402
from telemetry.edge import devices  # noqa: E402


@pytest.fixture(scope="session")
def demo_run(tmp_path_factory) -> tuple[Path, dict]:
    """One full simulated shift, shared by the end-to-end tests (about a minute)."""
    out = tmp_path_factory.mktemp("demo")
    report = demo.run(out, minutes=120, historian_days=2, log=lambda *_: None)
    return out, report


@pytest.fixture
def shift_end() -> datetime:
    return devices.SHIFT_START + timedelta(minutes=121)


def v2(device="PL01-L1-VIB-01", metric="vibration_mm_s", value=2.5, seq=0, event_time=None, **extra) -> dict:
    et = event_time or devices.SHIFT_START
    m = {
        "schema_version": 2,
        "message_id": f"m-{device}-{metric}-{seq}",
        "device_id": device,
        "plant_id": device[:4],
        "line_id": device[5:7],
        "metric": metric,
        "value": value,
        "unit": {"vibration_mm_s": "mm/s", "fryer_temp_c": "C"}.get(metric, "count"),
        "event_time": et.astimezone(UTC).isoformat(),
        "seq": seq,
        "quality": "GOOD",
    }
    return m | extra


def encode(m: dict) -> bytes:
    return json.dumps(m).encode()
