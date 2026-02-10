"""
Self-Driving performance metrics — SQL and definitions.

Metrics aligned with fleet data use cases: intervention rate,
disengagement rate, miles per intervention, event counts, etc.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config
import psycopg2
import pandas as pd


def get_conn():
    cfg = load_config()["timescaledb"]
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )


# --- Metric queries (return DataFrames) ---

def interventions_per_vehicle(vehicle_id=None, hours=24):
    """Number of interventions (and disengagements) per vehicle over last N hours."""
    with get_conn() as conn:
        params = [hours]
        q = """
        SELECT vehicle_id, event_type, COUNT(*) AS event_count
        FROM driving_events
        WHERE time > NOW() - (INTERVAL '1 hour' * %s)
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " AND vehicle_id = %s"
        q += " GROUP BY vehicle_id, event_type ORDER BY vehicle_id, event_type"
        return pd.read_sql(q, conn, params=params)


def alerts_summary(vehicle_id=None, limit=100):
    """Latest alerts by type."""
    with get_conn() as conn:
        params = []
        q = """
        SELECT time, vehicle_id, alert_type, alert_message
        FROM alerts
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " WHERE vehicle_id = %s"
        params.append(int(limit))
        q += " ORDER BY time DESC LIMIT %s"
        return pd.read_sql(q, conn, params=params)


def latest_telemetry(vehicle_id=None):
    """Latest telemetry row per vehicle (or single vehicle)."""
    with get_conn() as conn:
        if vehicle_id is not None:
            return pd.read_sql(
                "SELECT * FROM vehicle_telemetry WHERE vehicle_id = %s ORDER BY time DESC LIMIT 1",
                conn,
                params=[vehicle_id],
            )
        return pd.read_sql(
            "SELECT DISTINCT ON (vehicle_id) * FROM vehicle_telemetry ORDER BY vehicle_id, time DESC",
            conn,
        )


def miles_per_intervention(hours=24):
    """Approximate miles driven per intervention (Self-Driving metric)."""
    with get_conn() as conn:
        q = """
        WITH dist AS (
            SELECT vehicle_id, MAX(odometer_km) - MIN(odometer_km) AS km_driven
            FROM vehicle_telemetry
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
            GROUP BY vehicle_id
        ),
        interv AS (
            SELECT vehicle_id, COUNT(*) AS interventions
            FROM driving_events
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
              AND event_type IN ('intervention', 'disengagement')
            GROUP BY vehicle_id
        )
        SELECT d.vehicle_id,
               d.km_driven,
               COALESCE(i.interventions, 0) AS interventions,
               CASE WHEN COALESCE(i.interventions, 0) > 0
                    THEN d.km_driven / NULLIF(i.interventions, 0) ELSE NULL END AS km_per_intervention
        FROM dist d
        LEFT JOIN interv i ON d.vehicle_id = i.vehicle_id
        """
        return pd.read_sql(q, conn, params=[hours, hours])


def perception_summary(hours=24, vehicle_id=None):
    """Object detection summary: counts by class and vehicle."""
    with get_conn() as conn:
        params = [hours]
        q = """
        SELECT vehicle_id, object_class, COUNT(*) AS detection_count
        FROM perception_events
        WHERE time > NOW() - (INTERVAL '1 hour' * %s)
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " AND vehicle_id = %s"
        q += " GROUP BY vehicle_id, object_class ORDER BY vehicle_id, detection_count DESC"
        return pd.read_sql(q, conn, params=params)
