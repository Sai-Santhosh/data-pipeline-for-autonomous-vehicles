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


def distinct_vehicle_ids(limit=200):
    """List of vehicle_id values that have telemetry (for dropdowns; supports OpenSky/real sources)."""
    with get_conn() as conn:
        return pd.read_sql(
            "SELECT DISTINCT vehicle_id FROM vehicle_telemetry ORDER BY vehicle_id LIMIT %s",
            conn,
            params=[int(limit)],
        )["vehicle_id"].astype(int).tolist()


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


def intervention_rate_per_1000km(hours=24, vehicle_id=None):
    """Self-Driving metric: interventions per 1000 km driven (per vehicle). Lower is better."""
    with get_conn() as conn:
        params = [hours, hours]
        q = """
        WITH dist AS (
            SELECT vehicle_id, GREATEST(0, COALESCE(MAX(odometer_km), 0) - COALESCE(MIN(odometer_km), 0)) AS km_driven
            FROM vehicle_telemetry
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
            GROUP BY vehicle_id
        ),
        interv AS (
            SELECT vehicle_id, COUNT(*) AS interventions
            FROM driving_events
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
              AND event_type = 'intervention'
            GROUP BY vehicle_id
        )
        SELECT d.vehicle_id, d.km_driven, COALESCE(i.interventions, 0) AS interventions,
               CASE WHEN d.km_driven > 0 THEN (COALESCE(i.interventions, 0)::float / NULLIF(d.km_driven, 0)) * 1000 ELSE NULL END AS interventions_per_1000km
        FROM dist d
        LEFT JOIN interv i ON d.vehicle_id = i.vehicle_id
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " WHERE d.vehicle_id = %s"
        q += " ORDER BY d.vehicle_id"
        return pd.read_sql(q, conn, params=params)


def disengagement_rate_per_1000km(hours=24, vehicle_id=None):
    """Self-Driving metric: disengagements per 1000 km driven (per vehicle). Lower is better."""
    with get_conn() as conn:
        params = [hours, hours]
        q = """
        WITH dist AS (
            SELECT vehicle_id, GREATEST(0, COALESCE(MAX(odometer_km), 0) - COALESCE(MIN(odometer_km), 0)) AS km_driven
            FROM vehicle_telemetry
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
            GROUP BY vehicle_id
        ),
        diseng AS (
            SELECT vehicle_id, COUNT(*) AS disengagements
            FROM driving_events
            WHERE time > NOW() - (INTERVAL '1 hour' * %s)
              AND event_type = 'disengagement'
            GROUP BY vehicle_id
        )
        SELECT d.vehicle_id, d.km_driven, COALESCE(g.disengagements, 0) AS disengagements,
               CASE WHEN d.km_driven > 0 THEN (COALESCE(g.disengagements, 0)::float / NULLIF(d.km_driven, 0)) * 1000 ELSE NULL END AS disengagements_per_1000km
        FROM dist d
        LEFT JOIN diseng g ON d.vehicle_id = g.vehicle_id
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " WHERE d.vehicle_id = %s"
        q += " ORDER BY d.vehicle_id"
        return pd.read_sql(q, conn, params=params)


def fleet_self_driving_summary(hours=24):
    """Fleet-wide Self-Driving performance: total km, total interventions/disengagements, avg km per intervention."""
    with get_conn() as conn:
        q = """
        WITH per_vehicle_km AS (
            SELECT vehicle_id, GREATEST(0, COALESCE(MAX(odometer_km), 0) - COALESCE(MIN(odometer_km), 0)) AS km_driven
            FROM vehicle_telemetry WHERE time > NOW() - (INTERVAL '1 hour' * %s) GROUP BY vehicle_id
        ),
        tot_km AS (SELECT COALESCE(SUM(km_driven), 0) AS total_km FROM per_vehicle_km),
        interv AS (SELECT COUNT(*) AS n FROM driving_events WHERE time > NOW() - (INTERVAL '1 hour' * %s) AND event_type IN ('intervention', 'disengagement'))
        SELECT (SELECT total_km FROM tot_km) AS total_km_driven,
               (SELECT n FROM interv) AS total_interventions_plus_disengagements,
               CASE WHEN (SELECT n FROM interv) > 0 THEN (SELECT total_km FROM tot_km)::float / (SELECT n FROM interv) ELSE NULL END AS fleet_avg_km_per_intervention
        """
        return pd.read_sql(q, conn, params=[hours, hours])


def autopilot_engagement_rate(hours=24, vehicle_id=None):
    """% of telemetry records with autopilot_engaged = true (Self-Driving engagement metric)."""
    with get_conn() as conn:
        params = [hours]
        q = """
        SELECT vehicle_id,
               COUNT(*) AS total_records,
               SUM(CASE WHEN autopilot_engaged THEN 1 ELSE 0 END) AS engaged_records,
               ROUND(100.0 * SUM(CASE WHEN autopilot_engaged THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS engagement_pct
        FROM vehicle_telemetry
        WHERE time > NOW() - (INTERVAL '1 hour' * %s)
        """
        if vehicle_id is not None:
            params.append(vehicle_id)
            q += " AND vehicle_id = %s"
        q += " GROUP BY vehicle_id ORDER BY vehicle_id"
        return pd.read_sql(q, conn, params=params)
