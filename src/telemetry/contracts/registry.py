"""Data contracts: versioned JSON Schemas, validation, upcasting and a compatibility check.

Producers on the factory floor change (firmware updates); consumers must not break. The rules:
  * every message is validated against its declared contract version;
  * old versions are upcast to the current one at the edge, so central pipelines see one shape;
  * a new contract version may add optional fields but may not remove or retype a required
    field — `check_compatibility` enforces this in CI before a contract change can merge.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

SCHEMAS = Path(__file__).parent / "schemas"
CURRENT_VERSION = 2

V1_TAGS = {  # legacy tag -> (metric, unit, converter)
    "temp_f": ("fryer_temp_c", "C", lambda f: round((f - 32) * 5 / 9, 3)),
    "vib": ("vibration_mm_s", "mm/s", lambda x: x),
    "speed": ("line_speed_m_min", "m/min", lambda x: x),
    "power": ("power_kw", "kW", lambda x: x),
    "good": ("good_count", "count", lambda x: x),
    "reject": ("reject_count", "count", lambda x: x),
}


@cache
def schema(version: int) -> dict:
    return json.loads((SCHEMAS / f"sensor_reading.v{version}.json").read_text(encoding="utf-8"))


@cache
def validator(version: int) -> Draft202012Validator:
    return Draft202012Validator(schema(version), format_checker=FormatChecker())


def errors(message: dict, version: int) -> list[str]:
    return [f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in validator(version).iter_errors(message)]


def detect_version(message: dict) -> int | None:
    if "schema_version" in message:
        return message["schema_version"] if isinstance(message["schema_version"], int) else None
    if "dev" in message and "tag" in message:
        return 1
    return None


def upcast_v1(m: dict) -> dict:
    """Firmware 1.x → contract v2: rename fields, convert °F to °C, epoch ms to ISO time."""
    metric, unit, convert = V1_TAGS[m["tag"]]
    plant, line = m["dev"].split("-")[:2]
    return {
        "schema_version": 2,
        "message_id": m["id"],
        "device_id": m["dev"],
        "plant_id": plant,
        "line_id": line,
        "metric": metric,
        "value": convert(m["val"]),
        "unit": unit,
        "event_time": datetime.fromtimestamp(m["ts"] / 1000, tz=UTC).isoformat(),
        "seq": m["seq"],
        "quality": "UNCERTAIN" if m["val"] is None else "GOOD",
        "firmware": "1.x",
        **({"operator_ref": None} if "op" in m else {}),
    }


def check_compatibility(old: dict, new: dict) -> list[str]:
    """Backward-compatibility rules for a contract change (consumers of `old` must keep working)."""
    problems = []
    old_req, new_req = set(old.get("required", [])), set(new.get("required", []))
    for field in old_req - new_req:
        problems.append(f"required field '{field}' was removed or made optional")
    for field in new_req - old_req:
        if field not in old.get("properties", {}):
            problems.append(f"new required field '{field}' breaks existing producers")
    for field, spec in old.get("properties", {}).items():
        new_spec = new.get("properties", {}).get(field)
        if new_spec is None:
            if field in old_req:
                continue  # already reported
            problems.append(f"optional field '{field}' was removed")
            continue
        if spec.get("type") and new_spec.get("type") and _types(spec) - _types(new_spec):
            problems.append(f"field '{field}' changed type {spec.get('type')} -> {new_spec.get('type')}")
        if "enum" in spec and "enum" in new_spec and set(spec["enum"]) - set(new_spec["enum"]):
            problems.append(f"field '{field}' lost enum values {sorted(set(spec['enum']) - set(new_spec['enum']))}")
    return problems


def _types(spec: dict) -> set[str]:
    t = spec.get("type")
    return set(t) if isinstance(t, list) else {t}
