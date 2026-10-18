import copy

from telemetry.cli import main
from telemetry.contracts import registry

from .conftest import v2


def test_v2_message_is_valid():
    assert registry.errors(v2(), 2) == []


def test_v2_rejects_unknown_field_and_bad_device_id():
    assert registry.errors(v2(colour="red"), 2)
    assert registry.errors(v2(device="pl01-l1-vib-1"), 2)


def test_v1_upcast_converts_fahrenheit_and_epoch_millis():
    v1 = {"id": "5f0c6c1e9a2b4d7e8f10a1b2c3d4e5f6", "dev": "DE01-L2-TMP-01", "tag": "temp_f", "val": 347.0, "ts": 1_791_957_600_000, "seq": 4}
    assert registry.detect_version(v1) == 1 and registry.errors(v1, 1) == []
    up = registry.upcast_v1(v1)
    assert up["metric"] == "fryer_temp_c" and up["unit"] == "C"
    assert abs(up["value"] - 175.0) < 1e-6
    assert up["event_time"].startswith("2026-10-14T") and up["plant_id"] == "DE01" and up["line_id"] == "L2"
    assert registry.errors(up | {"gateway_id": "gw"}, 2) == []


def test_compatibility_allows_new_optional_field():
    old = registry.schema(2)
    new = copy.deepcopy(old)
    new["properties"]["battery_pct"] = {"type": "number"}
    assert registry.check_compatibility(old, new) == []


def test_compatibility_flags_breaking_changes():
    old = registry.schema(2)
    new = copy.deepcopy(old)
    new["properties"]["metric"]["enum"].remove("power_kw")
    new["required"].append("battery_pct")
    new["properties"]["battery_pct"] = {"type": "number"}
    new["properties"]["seq"]["type"] = "string"
    problems = registry.check_compatibility(old, new)
    assert any("lost enum values ['power_kw']" in p for p in problems)
    assert any("battery_pct" in p for p in problems)
    assert any("seq" in p for p in problems)


def test_contracts_check_cli_exit_code(tmp_path):
    import json

    old = registry.schema(2)
    broken = copy.deepcopy(old)
    del broken["properties"]["quality"]
    broken["required"].remove("quality")
    (tmp_path / "old.json").write_text(json.dumps(old))
    (tmp_path / "new.json").write_text(json.dumps(broken))
    assert main(["contracts-check", str(tmp_path / "old.json"), str(tmp_path / "old.json")]) == 0
    assert main(["contracts-check", str(tmp_path / "old.json"), str(tmp_path / "new.json")]) == 1
