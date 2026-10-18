"""Simulated factory floor: line controllers and sensors at three snack plants.

Each production line has five devices (fryer temperature, vibration, line speed, power, pack
counter). Readings are generated on a simulated clock so a two-hour shift runs in seconds and
the output is identical on every run (seeded).

Planted conditions — the things a real OT data platform has to cope with:
  * DE01 runs legacy firmware 1.x: different field names, Fahrenheit, epoch-ms timestamps;
  * PL01 line 2 develops a gearbox fault from minute 70: vibration climbs, rejects rise;
  * NL01 line 1 goes silent for 15 minutes (device offline → freshness alert);
  * the PL01 plant network drops for 25 minutes (gateway must buffer and replay; some data arrives late);
  * DE01-L3-TMP-01 has a clock running 7 minutes fast;
  * ~1% of messages are re-sent (MQTT QoS 1 redelivery), a few are corrupt or break the contract;
  * PL01-L3-TMP-01 occasionally reports a sensor fault (quality BAD, value 9999);
  * pack counters carry the operator's badge number (personal data).
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

SHIFT_START = datetime(2026, 10, 14, 6, 0, tzinfo=UTC)
INTERVAL_S = 10
PLANTS = {"DE01": "1.x", "NL01": "2.3.1", "PL01": "2.3.1"}
LINES = ["L1", "L2", "L3"]
DEVICE_KINDS = {  # kind -> metrics emitted
    "TMP": ["fryer_temp_c"],
    "VIB": ["vibration_mm_s"],
    "SPD": ["line_speed_m_min"],
    "PWR": ["power_kw"],
    "CNT": ["good_count", "reject_count"],
}
V1_TAG = {
    "fryer_temp_c": "temp_f",
    "vibration_mm_s": "vib",
    "line_speed_m_min": "speed",
    "power_kw": "power",
    "good_count": "good",
    "reject_count": "reject",
}

GEARBOX_FAULT = ("PL01", "L2", 70)  # plant, line, from minute
OFFLINE = ("NL01", "L1", 40, 55)  # plant, line, from, to minute
NETWORK_OUTAGE = ("PL01", 85, 110)  # plant, from, to minute — longer than the allowed lateness on purpose
CLOCK_DRIFT = ("DE01-L3-TMP-01", timedelta(minutes=7))
FAULTY_SENSOR = "PL01-L3-TMP-01"


@dataclass(frozen=True)
class Emission:
    """One message as it leaves a device: MQTT topic + payload bytes, and the true time it was sent."""

    sent_at: datetime
    plant_id: str
    topic: str
    payload: bytes


def device_ids() -> list[str]:
    return [f"{p}-{line}-{k}-01" for p in PLANTS for line in LINES for k in DEVICE_KINDS]


class Line:
    def __init__(self, plant: str, line: str, rng: random.Random):
        self.plant, self.line, self.rng = plant, line, rng
        self.seq: dict[str, int] = {}
        # Each line's normal operating point is fixed per line, so history and live data agree.
        base = random.Random(f"{plant}-{line}")
        self.base_temp = base.uniform(172, 178)
        self.base_speed = {"L1": 62.0, "L2": 55.0, "L3": 48.0}[line]
        self.base_vib = base.uniform(2.0, 2.8)

    def values(self, minute: float) -> dict[str, tuple[float, str]]:
        r = self.rng
        fault = (self.plant, self.line) == GEARBOX_FAULT[:2] and minute >= GEARBOX_FAULT[2]
        severity = min(1.0, (minute - GEARBOX_FAULT[2]) / 30) if fault else 0.0
        vib = self.base_vib + r.gauss(0, 0.15) + severity * r.uniform(4.0, 6.5)
        speed = self.base_speed * (1 - 0.15 * severity) + r.gauss(0, 0.4)
        good = max(0, round(speed / 60 * INTERVAL_S * 2.1 + r.gauss(0, 0.6)))
        reject = max(0, round(r.gauss(0.3, 0.4) + severity * r.uniform(1, 3)))
        return {
            "fryer_temp_c": (self.base_temp + r.gauss(0, 0.8), "GOOD"),
            "vibration_mm_s": (round(vib, 3), "GOOD"),
            "line_speed_m_min": (round(speed, 2), "GOOD"),
            "power_kw": (round(85 + speed * 0.9 + r.gauss(0, 1.5), 2), "GOOD"),
            "good_count": (float(good), "GOOD"),
            "reject_count": (float(reject), "GOOD"),
        }


def simulate(minutes: int = 120, seed: int = 7) -> Iterator[Emission]:
    """Yield every device emission of a shift in send order."""
    rng = random.Random(seed)
    lines = {(p, line): Line(p, line, rng) for p in PLANTS for line in LINES}
    steps = minutes * 60 // INTERVAL_S
    for step in range(steps):
        t = SHIFT_START + timedelta(seconds=step * INTERVAL_S)
        minute = step * INTERVAL_S / 60
        for (plant, line), ln in lines.items():
            if (plant, line) == OFFLINE[:2] and OFFLINE[2] <= minute < OFFLINE[3]:
                continue
            vals = ln.values(minute)
            for kind, metrics in DEVICE_KINDS.items():
                dev = f"{plant}-{line}-{kind}-01"
                device_clock = t + (CLOCK_DRIFT[1] if dev == CLOCK_DRIFT[0] else timedelta(0))
                for metric in metrics:
                    value, quality = vals[metric]
                    if dev == FAULTY_SENSOR and rng.random() < 0.01:
                        value, quality = 9999.0, "BAD"
                    seq = ln.seq.get(dev + metric, 0)
                    ln.seq[dev + metric] = seq + 1
                    badge = f"B-{rng.randint(100000, 100040)}" if kind == "CNT" else None
                    payload = _payload(plant, line, dev, metric, value, quality, device_clock, seq, badge)
                    em = Emission(t, plant, f"plant/{plant}/line/{line}/{dev}/{metric}", payload)
                    yield em
                    if rng.random() < 0.01:  # QoS 1 redelivery: same message again
                        yield em
        if step in (100, 300, 500, 600, 700):
            yield Emission(t, "PL01", "plant/PL01/line/L1/PL01-L1-SPD-01/line_speed_m_min", b'{"schema_version": 2, "message_id": "trunc')
        if step in (150, 350, 550, 650, 690):
            bad = {
                "schema_version": 2,
                "message_id": uuid.UUID(int=rng.getrandbits(128)).hex,
                "device_id": "NL01-L2-TMP-01",
                "plant_id": "NL01",
                "line_id": "L2",
                "metric": "humidity_pct",
                "value": "high",
                "unit": "%",
                "event_time": t.isoformat(),
                "seq": 0,
                "quality": "GOOD",
            }
            yield Emission(t, "NL01", "plant/NL01/line/L2/NL01-L2-TMP-01/humidity_pct", json.dumps(bad).encode())


def _payload(plant, line, dev, metric, value, quality, clock, seq, badge) -> bytes:
    mid = uuid.uuid5(uuid.NAMESPACE_URL, f"{dev}/{metric}/{seq}").hex
    if PLANTS[plant] == "1.x":
        v = round(value * 9 / 5 + 32, 3) if metric == "fryer_temp_c" else value
        msg = {"id": mid, "dev": dev, "tag": V1_TAG[metric], "val": v, "ts": int(clock.timestamp() * 1000), "seq": seq}
        if badge:
            msg["op"] = badge
        return json.dumps(msg).encode()
    msg = {
        "schema_version": 2,
        "message_id": mid,
        "device_id": dev,
        "plant_id": plant,
        "line_id": line,
        "metric": metric,
        "value": value,
        "unit": {"fryer_temp_c": "C", "vibration_mm_s": "mm/s", "line_speed_m_min": "m/min", "power_kw": "kW"}.get(metric, "count"),
        "event_time": clock.isoformat(),
        "seq": seq,
        "quality": quality,
        "firmware": PLANTS[plant],
    }
    if badge:
        msg["operator_badge"] = badge  # raw personal data: must be pseudonymised before it leaves the plant
    return json.dumps(msg).encode()


def historian_export(days: int = 7, seed: int = 11) -> list[dict]:
    """One-minute averages from the plant historian for the week before the shift (healthy operation).

    Wide format as OT historians export it: one row per minute, one column per tag.
    Used for batch backfill and to train the anomaly baseline.
    """
    rng = random.Random(seed)
    lines = {(p, line): Line(p, line, rng) for p in PLANTS for line in LINES}
    start = SHIFT_START - timedelta(days=days)
    rows = []
    for m in range(days * 24 * 60):
        t = start + timedelta(minutes=m)
        row: dict = {"timestamp": t.isoformat()}
        for (plant, line), ln in lines.items():
            vals = ln.values(-1.0)  # before the fault
            for metric in ("fryer_temp_c", "vibration_mm_s", "line_speed_m_min", "power_kw"):
                kind = {"fryer_temp_c": "TMP", "vibration_mm_s": "VIB", "line_speed_m_min": "SPD", "power_kw": "PWR"}[metric]
                row[f"{plant}-{line}-{kind}-01.{metric}"] = round(vals[metric][0], 3)
        rows.append(row)
    return rows
