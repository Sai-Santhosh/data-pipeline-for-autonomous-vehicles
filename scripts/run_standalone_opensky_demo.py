#!/usr/bin/env python3
"""
Standalone demo: fetch real OpenSky data, write to SQLite, export results.
No Docker/Kafka required. Run: python scripts/run_standalone_opensky_demo.py
"""
import sqlite3
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.logging_config import setup_logging, LOG_FILE

setup_logging(log_file=True, console=True)
import logging
logger = logging.getLogger(__name__)

RESULTS = ROOT / "results"
DB_PATH = ROOT / "results" / "demo_opensky.db"

def _ts_from_unix(unix_sec):
    if unix_sec is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.fromtimestamp(unix_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _vehicle_id(icao24):
    if not icao24:
        return 1
    n = int(icao24, 16) if isinstance(icao24, str) else hash(icao24)
    return (n & 0x7FFF_FFFF) % 9999 + 1

IDX = dict(icao24=0, callsign=1, origin_country=2, time_position=3, lon=5, lat=6, velocity=9)

def main():
    logger.info("=== Standalone OpenSky demo (real data, no Docker) === Log file: %s", LOG_FILE)
    RESULTS.mkdir(exist_ok=True)

    # 1) Fetch OpenSky
    logger.info("Fetching real data from OpenSky Network API...")
    try:
        import requests
        r = requests.get("https://opensky-network.org/api/states/all", timeout=15)
        r.raise_for_status()
        data = r.json()
        states = data.get("states") or []
    except Exception as e:
        logger.exception("OpenSky fetch failed: %s", e)
        return 1
    logger.info("Received %s aircraft", len(states))

    # 2) SQLite schema
    conn = sqlite3.connect(DB_PATH)
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
    CREATE TABLE IF NOT EXISTS alerts (
        time TEXT, vehicle_id INT, alert_type TEXT, alert_message TEXT
    );
    DELETE FROM vehicle_telemetry;
    DELETE FROM perception_events;
    DELETE FROM alerts;
    """)
    conn.commit()

    # 3) Map and insert (first 25 aircraft)
    to_emit = states[:25]
    for s in to_emit:
        if not s or len(s) <= IDX["velocity"]:
            continue
        lat, lon = s[IDX["lat"]], s[IDX["lon"]]
        if lat is None or lon is None:
            continue
        vel = s[IDX["velocity"]] or 0
        speed_kmh = round(vel * 3.6, 2)
        vid = _vehicle_id(s[IDX["icao24"]])
        ts = _ts_from_unix(s[IDX["time_position"]])
        conn.execute(
            "INSERT INTO vehicle_telemetry (time, vehicle_id, current_speed_kmh, speed_limit_violation, "
            "latitude, longitude, battery_level_pct, remaining_range_km, autopilot_engaged, odometer_km, "
            "start_location, destination) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (ts, vid, speed_kmh, 1 if speed_kmh > 65 else 0, lat, lon, 100.0, 500.0, 1, 0.0,
             s[IDX["origin_country"]] or "N/A", (s[IDX["callsign"]] or "").strip() or "N/A"),
        )
        if speed_kmh > 65:
            conn.execute(
                "INSERT INTO alerts (time, vehicle_id, alert_type, alert_message) VALUES (?,?,?,?)",
                (ts, vid, "Speed Violation", f"Vehicle {vid} exceeded speed limit ({speed_kmh:.0f} km/h)")
            )
    conn.commit()
    logger.info("Wrote %s telemetry rows and alerts to SQLite", len(to_emit))

    # 4) Export to CSV in results/
    cur = conn.cursor()
    cur.execute("SELECT * FROM vehicle_telemetry")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    import csv
    with open(RESULTS / "latest_telemetry.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    logger.info("Exported results/latest_telemetry.csv")

    cur.execute("SELECT * FROM alerts")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    with open(RESULTS / "alerts_sample.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    logger.info("Exported results/alerts_sample.csv")

    # Summary CSV
    cur.execute("SELECT vehicle_id, COUNT(*) as cnt, MAX(current_speed_kmh) as max_speed FROM vehicle_telemetry GROUP BY vehicle_id")
    rows = cur.fetchall()
    with open(RESULTS / "telemetry_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["vehicle_id", "record_count", "max_speed_kmh"])
        w.writerows(rows)
    logger.info("Exported results/telemetry_summary.csv")

    conn.close()
    logger.info("Done. Open results/ for CSVs. Log file: %s", LOG_FILE)
    return 0

if __name__ == "__main__":
    sys.exit(main())
