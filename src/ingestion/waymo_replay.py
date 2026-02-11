"""
Waymo-style data replay: publish telemetry (and optional perception) from file to Kafka.

Waymo Open Dataset does not provide a live API; data is batch (TFRecord on GCS).
This module replays from CSV or JSONL files that match our pipeline schema, so you can:
  - Export Waymo Open Dataset (e.g. Motion Dataset) to CSV/JSONL, or
  - Use the included sample CSV for demo.
"""
from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config

logger = logging.getLogger(__name__)

# Required telemetry columns (others get defaults)
TELEMETRY_COLS = [
    "vehicle_id", "timestamp", "current_speed_kmh", "speed_limit_violation",
    "latitude", "longitude", "battery_level_pct", "remaining_range_km",
    "autopilot_engaged", "odometer_km", "start_location", "destination",
]
DEFAULTS = {
    "speed_limit_violation": False,
    "battery_level_pct": 85.0,
    "remaining_range_km": 400.0,
    "autopilot_engaged": True,
    "odometer_km": 0.0,
    "start_location": "Waymo",
    "destination": "Waymo",
}


def _normalize_telemetry_row(row: dict) -> dict:
    """Ensure row has all keys and types for vehicle_telemetry."""
    out = {}
    for k in TELEMETRY_COLS:
        v = row.get(k)
        if v is None or v == "":
            v = DEFAULTS.get(k)
        if k in ("vehicle_id",):
            out[k] = int(float(v)) if v is not None else 1
        elif k in ("current_speed_kmh", "latitude", "longitude", "battery_level_pct", "remaining_range_km", "odometer_km"):
            out[k] = float(v) if v is not None else 0.0
        elif k == "speed_limit_violation":
            out[k] = str(v).lower() in ("true", "1", "yes") if v is not None else False
        elif k == "autopilot_engaged":
            out[k] = str(v).lower() not in ("false", "0", "no") if v is not None else True
        else:
            out[k] = str(v) if v is not None else DEFAULTS.get(k, "")
    return out


def read_telemetry_csv(path: Path) -> list[dict]:
    """Read telemetry rows from CSV. First row = header."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(_normalize_telemetry_row(r))
    return rows


def read_telemetry_jsonl(path: Path) -> list[dict]:
    """Read telemetry rows from JSONL (one JSON object per line)."""
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(_normalize_telemetry_row(json.loads(line)))
    return rows


def read_telemetry_file(path: Path) -> list[dict]:
    """Dispatch by extension: .csv or .jsonl."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".csv":
        return read_telemetry_csv(path)
    if path.suffix.lower() in (".jsonl", ".ndjson"):
        return read_telemetry_jsonl(path)
    raise ValueError("Use .csv or .jsonl file")


def run_waymo_replay(
    telemetry_path: Path | str,
    perception_path: Path | str | None = None,
    speed_factor: float = 1.0,
    loop: bool = False,
) -> None:
    """
    Replay telemetry (and optional perception) from file to Kafka.
    speed_factor: 1.0 = send as fast as possible; 0.1 = 10x slower (simulate real-time).
    loop: if True, replay file repeatedly.
    """
    cfg = load_config()
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic_telemetry = cfg["kafka"]["topics"]["vehicle_telemetry"]
    topic_perception = cfg["kafka"]["topics"]["perception_events"]

    telemetry_path = Path(telemetry_path)
    telemetry_rows = read_telemetry_file(telemetry_path)
    if not telemetry_rows:
        logger.warning("No telemetry rows in %s", telemetry_path)
        producer.close()
        return

    perception_rows = []
    if perception_path and Path(perception_path).exists():
        with open(perception_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    perception_rows.append(json.loads(line))
        logger.info("Loaded %s perception rows", len(perception_rows))

    sent = 0
    try:
        while True:
            prev_ts = None
            for row in telemetry_rows:
                producer.send(topic_telemetry, row)
                sent += 1
                if speed_factor < 1.0 and prev_ts is not None and row.get("timestamp"):
                    try:
                        # Simple delay between rows if timestamps available
                        t = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
                        delta = (t.timestamp() - prev_ts) * (1.0 - speed_factor)
                        if delta > 0:
                            time.sleep(delta)
                    except Exception:
                        time.sleep(0.1)
                    prev_ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00")).timestamp()
                elif speed_factor < 1.0:
                    time.sleep(0.1 * (1.0 - speed_factor))
            for row in perception_rows:
                producer.send(topic_perception, row)
                sent += 1
            if not loop:
                break
            logger.info("Replay loop: sent %s so far", sent)
    except KeyboardInterrupt:
        pass
    producer.flush()
    producer.close()
    logger.info("Waymo replay done. Sent %s messages.", sent)
