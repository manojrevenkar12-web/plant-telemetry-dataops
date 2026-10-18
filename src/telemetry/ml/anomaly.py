"""Vibration anomaly model: a per-device robust baseline (median and MAD) learned from the
historian's healthy week, scored on live minute averages.

Deliberately simple and explainable — the point of this repository is the data pipeline that
feeds a model, versions its training data and serves it, not the model itself. The model card
records exactly which Delta table version it was trained on, so a score can always be traced
back to its training data.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ..lake.tables import Lake

THRESHOLD = 6.0  # robust z-score
METRIC = "vibration_mm_s"


def train(lake: Lake, registry_dir: Path) -> dict:
    hist = lake.read("silver/historian_1m", filters=[("metric", "=", METRIC)])
    if hist.is_empty():
        raise ValueError("no historian data to train on; run the backfill first")
    stats = hist.group_by("device_id").agg(
        pl.col("value").median().alias("median"), (pl.col("value") - pl.col("value").median()).abs().median().alias("mad"), pl.len().alias("n")
    )
    versions = sorted(int(p.stem.split("v")[-1]) for p in registry_dir.glob("vibration_anomaly_v*.json")) if registry_dir.exists() else []
    version = (versions[-1] + 1) if versions else 1
    card = {
        "name": "vibration_anomaly",
        "version": version,
        "trained_at": datetime.now(UTC).isoformat(),
        "algorithm": "robust z-score: |x − median| / (1.4826 · MAD) per device",
        "threshold": THRESHOLD,
        "training_data": {
            "table": "silver/historian_1m",
            "delta_version": lake.version("silver/historian_1m"),
            "rows": hist.height,
            "from": str(hist["minute"].min()),
            "to": str(hist["minute"].max()),
        },
        "devices": {r["device_id"]: {"median": r["median"], "mad": max(r["mad"], 1e-3), "n": r["n"]} for r in stats.iter_rows(named=True)},
    }
    registry_dir.mkdir(parents=True, exist_ok=True)
    (registry_dir / f"vibration_anomaly_v{version}.json").write_text(json.dumps(card, indent=2))
    return card


def load_latest(registry_dir: Path) -> dict | None:
    cards = sorted(registry_dir.glob("vibration_anomaly_v*.json"), key=lambda p: int(p.stem.split("v")[-1])) if registry_dir.exists() else []
    return json.loads(cards[-1].read_text()) if cards else None


def score(card: dict, device_id: str, values: list[float]) -> list[dict]:
    d = card["devices"].get(device_id)
    if d is None:
        raise KeyError(f"model has no baseline for {device_id}")
    out = []
    for v in values:
        z = abs(v - d["median"]) / (1.4826 * d["mad"])
        out.append({"value": v, "score": round(z, 2), "anomaly": z > card["threshold"]})
    return out


def evaluate(card: dict, lake: Lake, fault_device: str, fault_from: datetime) -> dict:
    """Score every live minute and compare with the planted gearbox fault (ground truth).

    Reports recall over the fault window, false alarms on healthy device-minutes, and how many
    minutes after the fault began the first alert fired (the fault ramps up over 30 minutes, so
    the very first minutes are not expected to be caught).
    """
    gold = lake.read("gold/metrics_1m", filters=[("metric", "=", METRIC), ("source", "=", "stream")]).drop_nulls("avg")
    tp = fp = fn = tn = 0
    first_alert = None
    for r in gold.sort("minute").iter_rows(named=True):
        if r["device_id"] not in card["devices"]:
            continue
        flagged = score(card, r["device_id"], [r["avg"]])[0]["anomaly"]
        truth = r["device_id"] == fault_device and r["minute"] >= fault_from
        tp += flagged and truth
        fp += flagged and not truth
        fn += (not flagged) and truth
        tn += (not flagged) and not truth
        if flagged and truth and first_alert is None:
            first_alert = r["minute"]
    return {
        "precision": round(tp / (tp + fp), 3) if tp + fp else None,
        "recall": round(tp / (tp + fn), 3) if tp + fn else None,
        "false_alarms": fp,
        "healthy_minutes_scored": fp + tn,
        "first_alert_minutes_after_onset": round((first_alert - fault_from).total_seconds() / 60) if first_alert else None,
        "tp": tp,
        "fn": fn,
    }
