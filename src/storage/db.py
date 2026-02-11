"""
TimescaleDB connection and write helpers for vehicle telemetry and alerts.
"""
import logging
from contextlib import contextmanager
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_connection_params():
    cfg = load_config()["timescaledb"]
    return {
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["database"],
        "user": cfg["user"],
        "password": cfg["password"],
    }


@contextmanager
def get_conn():
    params = get_connection_params()
    conn = psycopg2.connect(**params)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_database():
    """Create fleet_data database if it does not exist (connect to postgres first)."""
    cfg = load_config()["timescaledb"]
    dbname = cfg["database"]
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname="postgres",
        user=cfg["user"],
        password=cfg["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (dbname,),
    )
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{dbname}"')
        logger.info("Created database %s", dbname)
    cur.close()
    conn.close()


def insert_vehicle_telemetry(conn, rows: list[dict]) -> None:
    """Bulk insert vehicle_telemetry. Each row must have keys matching table columns."""
    if not rows:
        return
    cols = [
        "time", "vehicle_id", "current_speed_kmh", "speed_limit_violation",
        "latitude", "longitude", "battery_level_pct", "remaining_range_km",
        "autopilot_engaged", "odometer_km", "start_location", "destination",
    ]
    values = [
        (
            r["timestamp"], r["vehicle_id"], r["current_speed_kmh"], r.get("speed_limit_violation", False),
            r["latitude"], r["longitude"], r["battery_level_pct"], r["remaining_range_km"],
            r.get("autopilot_engaged", True), r.get("odometer_km"), r.get("start_location"), r.get("destination"),
        )
        for r in rows
    ]
    execute_values(
        conn,
        "INSERT INTO vehicle_telemetry (" + ", ".join(cols) + ") VALUES %s",
        values,
        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        page_size=500,
    )


def insert_perception_events(conn, rows: list[dict]) -> None:
    if not rows:
        return
    cols = [
        "time", "vehicle_id", "object_class", "object_distance_m", "object_speed_kmh",
        "object_relative_direction", "confidence",
    ]
    values = [
        (
            r["timestamp"], r["vehicle_id"], r["object_class"], r["object_distance_m"],
            r["object_speed_kmh"], r.get("object_relative_direction", "ahead"), r.get("confidence", 1.0),
        )
        for r in rows
    ]
    execute_values(
        conn,
        "INSERT INTO perception_events (" + ", ".join(cols) + ") VALUES %s",
        values,
        template="(%s, %s, %s, %s, %s, %s, %s)",
        page_size=500,
    )


def insert_driving_events(conn, rows: list[dict]) -> None:
    if not rows:
        return
    values = [
        (r["timestamp"], r["vehicle_id"], r["event_type"], r.get("event_detail"), r.get("latitude"), r.get("longitude"))
        for r in rows
    ]
    execute_values(
        conn,
        "INSERT INTO driving_events (time, vehicle_id, event_type, event_detail, latitude, longitude) VALUES %s",
        values,
        template="(%s, %s, %s, %s, %s, %s)",
        page_size=500,
    )


def insert_alert(conn, vehicle_id: int, alert_type: str, alert_message: str, time_ts=None) -> None:
    from datetime import datetime, timezone
    t = time_ts or datetime.now(timezone.utc)
    if isinstance(t, str):
        try:
            t = datetime.fromisoformat(t.replace("Z", "+00:00"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
        except Exception:
            t = datetime.now(timezone.utc)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO alerts (time, vehicle_id, alert_type, alert_message) VALUES (%s, %s, %s, %s)",
        (t, vehicle_id, alert_type, alert_message),
    )
    cur.close()
