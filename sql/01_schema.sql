-- Fleet Data Pipeline — TimescaleDB schema for Self-Driving metrics
-- Run against database: fleet_data (or postgres)

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Vehicle telemetry (time-series)
CREATE TABLE IF NOT EXISTS vehicle_telemetry (
    time        TIMESTAMPTZ NOT NULL,
    vehicle_id  INT NOT NULL,
    current_speed_kmh       DOUBLE PRECISION,
    speed_limit_violation   BOOLEAN DEFAULT FALSE,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    battery_level_pct      DOUBLE PRECISION,
    remaining_range_km     DOUBLE PRECISION,
    autopilot_engaged      BOOLEAN DEFAULT TRUE,
    odometer_km            DOUBLE PRECISION,
    start_location         TEXT,
    destination            TEXT
);

SELECT create_hypertable('vehicle_telemetry', 'time', if_not_exists => TRUE);

-- Perception/detection events (time-series)
CREATE TABLE IF NOT EXISTS perception_events (
    time           TIMESTAMPTZ NOT NULL,
    vehicle_id     INT NOT NULL,
    object_class   VARCHAR(50),
    object_distance_m   DOUBLE PRECISION,
    object_speed_kmh    DOUBLE PRECISION,
    object_relative_direction VARCHAR(20),
    confidence     DOUBLE PRECISION
);

SELECT create_hypertable('perception_events', 'time', if_not_exists => TRUE);

-- Driving events: interventions, disengagements (Self-Driving metrics)
CREATE TABLE IF NOT EXISTS driving_events (
    time         TIMESTAMPTZ NOT NULL,
    vehicle_id   INT NOT NULL,
    event_type   VARCHAR(50) NOT NULL,
    event_detail TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION
);

SELECT create_hypertable('driving_events', 'time', if_not_exists => TRUE);

-- Alerts (speed, battery, collision risk) — written by stream processor
CREATE TABLE IF NOT EXISTS alerts (
    id          SERIAL,
    time        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    vehicle_id  INT NOT NULL,
    alert_type  VARCHAR(80) NOT NULL,
    alert_message TEXT
);

SELECT create_hypertable('alerts', 'time', if_not_exists => TRUE);

-- Aggregated Self-Driving metrics (optional; can be materialized from queries)
CREATE TABLE IF NOT EXISTS self_driving_metrics (
    time_bucket  TIMESTAMPTZ NOT NULL,
    vehicle_id   INT,
    metric_name  VARCHAR(80) NOT NULL,
    metric_value DOUBLE PRECISION,
    metadata     JSONB
);

SELECT create_hypertable('self_driving_metrics', 'time_bucket', if_not_exists => TRUE);

-- Indexes for common filters
CREATE INDEX IF NOT EXISTS idx_vehicle_telemetry_vehicle_id ON vehicle_telemetry (vehicle_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_perception_events_vehicle_id ON perception_events (vehicle_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_driving_events_vehicle_type ON driving_events (vehicle_id, event_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_vehicle_time ON alerts (vehicle_id, time DESC);
