#!/usr/bin/env python3
"""
Full pipeline (no Docker): Producer -> Queue -> Consumer -> SQLite -> Export.

Simulates the complete flow without Kafka/TimescaleDB:
  1. Producer: fetch OpenSky API, map to telemetry/perception, put in queue
  2. Consumer: read from queue, apply alert rules, batch insert to SQLite
  3. Export: CSVs to results/

Run: python scripts/run_full_pipeline_local.py
"""
import csv
import logging
import queue
import sqlite3
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.logging_config import setup_logging, LOG_FILE
from src.ingestion.opensky_source import (
    fetch_opensky_states,
    opensky_state_to_telemetry,
    opensky_state_to_perception,
)
from config import load_config

setup_logging(log_file=True, console=True)
logger = logging.getLogger(__name__)

RESULTS = ROOT / "results"
DB_PATH = ROOT / "results" / "full_pipeline_local.db"
SENTINEL = object()  # poison pill to stop consumer

BATCH_SIZE = 50


def create_schema(conn: sqlite3.Connection):
    """Create SQLite schema matching TimescaleDB tables."""
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS vehicle_telemetry (
        time TEXT, vehicle_id INT, current_speed_kmh REAL, speed_limit_violation INT,
        latitude REAL, longitude REAL, battery_level_pct REAL, remaining_range_km REAL,
        autopilot_engaged INT, odometer_km REAL, start_location TEXT, destination TEXT
    );
    CREATE TABLE IF NOT EXISTS perception_events (
        time TEXT, vehicle_id INT, object_class TEXT, object_distance_m REAL,
        object_speed_kmh REAL, object_relative_direction TEXT, confidence REAL
    );
    CREATE TABLE IF NOT EXISTS driving_events (
        time TEXT, vehicle_id INT, event_type TEXT, event_detail TEXT, latitude REAL, longitude REAL
    );
    CREATE TABLE IF NOT EXISTS alerts (
        time TEXT, vehicle_id INT, alert_type TEXT, alert_message TEXT
    );
    DELETE FROM vehicle_telemetry;
    DELETE FROM perception_events;
    DELETE FROM driving_events;
    DELETE FROM alerts;
    """)


def run_producer(msg_queue: queue.Queue, max_polls: int, max_vehicles: int):
    """Producer: fetch OpenSky, map, put (topic, payload) in queue."""
    cfg = load_config()
    bbox = cfg.get("opensky", {}).get("bbox")
    battery_threshold = cfg.get("metrics", {}).get("intervention_battery_threshold_pct", 20)
    collision_risk_m = cfg.get("metrics", {}).get("collision_risk_distance_m", 5.0)

    for poll_num in range(max_polls):
        states = fetch_opensky_states(bbox)
        if not states:
            logger.info("Producer: no states this poll")
            time.sleep(2)
            continue
        to_emit = states[:max_vehicles]
        for state in to_emit:
            tel = opensky_state_to_telemetry(state)
            if tel:
                msg_queue.put(("vehicle_telemetry", tel))
            for rec in opensky_state_to_perception(state, to_emit):
                msg_queue.put(("perception_events", rec))
        logger.info("Producer: emitted %s vehicles (poll %s/%s)", len(to_emit), poll_num + 1, max_polls)
        if poll_num < max_polls - 1:
            time.sleep(2)
    msg_queue.put(SENTINEL)


def run_consumer(msg_queue: queue.Queue, db_path: Path):
    """Consumer: read from queue, apply alerts, batch insert."""
    conn = sqlite3.connect(db_path)
    cfg = load_config()
    battery_threshold = cfg.get("metrics", {}).get("intervention_battery_threshold_pct", 20)
    collision_risk_m = cfg.get("metrics", {}).get("collision_risk_distance_m", 5.0)

    telemetry_buf = []
    perception_buf = []
    driving_buf = []

    while True:
        try:
            item = msg_queue.get(timeout=0.5)
        except queue.Empty:
            # Flush if producer might be done
            continue
        if item is SENTINEL:
            break
        topic, payload = item
        if topic == "vehicle_telemetry":
            telemetry_buf.append(payload)
            if payload.get("speed_limit_violation"):
                conn.execute(
                    "INSERT INTO alerts (time, vehicle_id, alert_type, alert_message) VALUES (?,?,?,?)",
                    (
                        payload.get("timestamp"),
                        payload["vehicle_id"],
                        "Speed Violation",
                        f"Vehicle {payload['vehicle_id']} exceeded speed limit ({payload.get('current_speed_kmh', 0):.0f} km/h)",
                    ),
                )
            if payload.get("battery_level_pct", 100) < battery_threshold:
                conn.execute(
                    "INSERT INTO alerts (time, vehicle_id, alert_type, alert_message) VALUES (?,?,?,?)",
                    (
                        payload.get("timestamp"),
                        payload["vehicle_id"],
                        "Low Battery",
                        f"Vehicle {payload['vehicle_id']} battery low: {payload.get('battery_level_pct', 0):.1f}%",
                    ),
                )
        elif topic == "perception_events":
            perception_buf.append(payload)
            if payload.get("object_distance_m", 999) < collision_risk_m and payload.get("object_speed_kmh", 0) > 10:
                conn.execute(
                    "INSERT INTO alerts (time, vehicle_id, alert_type, alert_message) VALUES (?,?,?,?)",
                    (
                        payload.get("timestamp"),
                        payload["vehicle_id"],
                        "Collision Risk",
                        f"Vehicle {payload['vehicle_id']} detected {payload.get('object_class', 'object')} "
                        f"at {payload.get('object_distance_m', 0):.1f}m",
                    ),
                )
        elif topic == "driving_events":
            driving_buf.append(payload)

        # Batch insert
        if len(telemetry_buf) >= BATCH_SIZE or len(perception_buf) >= BATCH_SIZE or len(driving_buf) >= BATCH_SIZE:
            for row in telemetry_buf:
                conn.execute(
                    "INSERT INTO vehicle_telemetry (time, vehicle_id, current_speed_kmh, speed_limit_violation, "
                    "latitude, longitude, battery_level_pct, remaining_range_km, autopilot_engaged, odometer_km, "
                    "start_location, destination) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        row["timestamp"], row["vehicle_id"], row["current_speed_kmh"],
                        1 if row.get("speed_limit_violation") else 0,
                        row["latitude"], row["longitude"], row["battery_level_pct"], row["remaining_range_km"],
                        1 if row.get("autopilot_engaged", True) else 0, row.get("odometer_km", 0),
                        row.get("start_location"), row.get("destination"),
                    ),
                )
            for row in perception_buf:
                conn.execute(
                    "INSERT INTO perception_events (time, vehicle_id, object_class, object_distance_m, "
                    "object_speed_kmh, object_relative_direction, confidence) VALUES (?,?,?,?,?,?,?)",
                    (
                        row["timestamp"], row["vehicle_id"], row.get("object_class"),
                        row.get("object_distance_m"), row.get("object_speed_kmh"),
                        row.get("object_relative_direction"), row.get("confidence"),
                    ),
                )
            for row in driving_buf:
                conn.execute(
                    "INSERT INTO driving_events (time, vehicle_id, event_type, event_detail, latitude, longitude) VALUES (?,?,?,?,?,?)",
                    (row.get("timestamp"), row["vehicle_id"], row.get("event_type"), row.get("event_detail"),
                     row.get("latitude"), row.get("longitude")),
                )
            conn.commit()
            logger.info("Consumer: wrote %s telemetry, %s perception, %s driving", len(telemetry_buf), len(perception_buf), len(driving_buf))
            telemetry_buf, perception_buf, driving_buf = [], [], []

    # Flush remainder
    for row in telemetry_buf:
        conn.execute(
            "INSERT INTO vehicle_telemetry (time, vehicle_id, current_speed_kmh, speed_limit_violation, "
            "latitude, longitude, battery_level_pct, remaining_range_km, autopilot_engaged, odometer_km, "
            "start_location, destination) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                row["timestamp"], row["vehicle_id"], row["current_speed_kmh"],
                1 if row.get("speed_limit_violation") else 0,
                row["latitude"], row["longitude"], row["battery_level_pct"], row["remaining_range_km"],
                1 if row.get("autopilot_engaged", True) else 0, row.get("odometer_km", 0),
                row.get("start_location"), row.get("destination"),
            ),
        )
    for row in perception_buf:
        conn.execute(
            "INSERT INTO perception_events (time, vehicle_id, object_class, object_distance_m, "
            "object_speed_kmh, object_relative_direction, confidence) VALUES (?,?,?,?,?,?,?)",
            (row["timestamp"], row["vehicle_id"], row.get("object_class"), row.get("object_distance_m"),
             row.get("object_speed_kmh"), row.get("object_relative_direction"), row.get("confidence")),
        )
    for row in driving_buf:
        conn.execute(
            "INSERT INTO driving_events (time, vehicle_id, event_type, event_detail, latitude, longitude) VALUES (?,?,?,?,?,?)",
            (row.get("timestamp"), row["vehicle_id"], row.get("event_type"), row.get("event_detail"),
             row.get("latitude"), row.get("longitude")),
        )
    conn.commit()
    conn.close()
    logger.info("Consumer: flush complete")


def export_results(db_path: Path):
    """Export SQLite tables to CSVs in results/."""
    conn = sqlite3.connect(db_path)
    RESULTS.mkdir(exist_ok=True)
    for table, filename in [
        ("vehicle_telemetry", "latest_telemetry.csv"),
        ("alerts", "alerts_sample.csv"),
        ("perception_events", "perception_summary_24h.csv"),
    ]:
        cur = conn.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        with open(RESULTS / filename, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        logger.info("Exported %s", filename)
    cur = conn.execute(
        "SELECT vehicle_id, COUNT(*) as record_count, MAX(current_speed_kmh) as max_speed_kmh "
        "FROM vehicle_telemetry GROUP BY vehicle_id"
    )
    rows = cur.fetchall()
    with open(RESULTS / "telemetry_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["vehicle_id", "record_count", "max_speed_kmh"])
        w.writerows(rows)
    logger.info("Exported telemetry_summary.csv")
    conn.close()


def main():
    max_polls = 3
    max_vehicles = 25

    logger.info("=== Full pipeline (local, no Docker) ===")
    logger.info("Log file: %s", LOG_FILE)
    RESULTS.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    conn.commit()
    conn.close()

    msg_queue = queue.Queue()
    producer = threading.Thread(target=run_producer, args=(msg_queue, max_polls, max_vehicles))
    consumer = threading.Thread(target=run_consumer, args=(msg_queue, DB_PATH))

    consumer.start()
    producer.start()
    producer.join()
    consumer.join()

    export_results(DB_PATH)

    logger.info("Done. Check results/ for CSVs. Dashboard requires Docker (Kafka+TimescaleDB).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
