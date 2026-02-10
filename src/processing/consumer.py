"""
Stream consumer: Kafka -> metrics & alerts -> TimescaleDB.

Consumes vehicle_telemetry, perception_events, driving_events; applies
Self-Driving metric rules (speed violation, low battery, collision risk);
writes telemetry, perception, driving events and alerts to TimescaleDB.
"""
import json
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config
from src.storage.db import (
    get_conn,
    insert_vehicle_telemetry,
    insert_perception_events,
    insert_driving_events,
    insert_alert,
)

try:
    from kafka import KafkaConsumer
except ImportError:
    raise ImportError("Install kafka-python: pip install kafka-python")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
SHUTDOWN = False


def main():
    cfg = load_config()
    kafka_cfg = cfg["kafka"]
    metrics_cfg = cfg.get("metrics", {})

    bootstrap = kafka_cfg["bootstrap_servers"]
    topic_telemetry = kafka_cfg["topics"]["vehicle_telemetry"]
    topic_perception = kafka_cfg["topics"]["perception_events"]
    topic_driving = kafka_cfg["topics"]["driving_events"]

    battery_threshold = metrics_cfg.get("intervention_battery_threshold_pct", 20)
    speed_limit_kmh = metrics_cfg.get("speed_limit_violation_kmh", 65)
    collision_risk_m = metrics_cfg.get("collision_risk_distance_m", 5.0)

    consumer = KafkaConsumer(
        topic_telemetry,
        topic_perception,
        topic_driving,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="fleet_pipeline_consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    def on_signal(*_):
        nonlocal SHUTDOWN
        SHUTDOWN = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    telemetry_buf = []
    perception_buf = []
    driving_buf = []

    logger.info("Consumer started: %s", bootstrap)

    for message in consumer:
        if SHUTDOWN:
            break
        try:
            payload = message.value
            topic = message.topic

            if topic == topic_telemetry:
                telemetry_buf.append(payload)
                # Alerts from telemetry
                if payload.get("speed_limit_violation"):
                    with get_conn() as conn:
                        insert_alert(
                            conn,
                            payload["vehicle_id"],
                            "Speed Violation",
                            f"Vehicle {payload['vehicle_id']} exceeded speed limit ({payload.get('current_speed_kmh', 0):.0f} km/h)",
                            payload.get("timestamp"),
                        )
                if payload.get("battery_level_pct", 100) < battery_threshold:
                    with get_conn() as conn:
                        insert_alert(
                            conn,
                            payload["vehicle_id"],
                            "Low Battery",
                            f"Vehicle {payload['vehicle_id']} battery low: {payload.get('battery_level_pct', 0):.1f}%",
                            payload.get("timestamp"),
                        )

            elif topic == topic_perception:
                perception_buf.append(payload)
                # Collision risk: close and fast-moving object
                if (
                    payload.get("object_distance_m", 999) < collision_risk_m
                    and payload.get("object_speed_kmh", 0) > 10
                ):
                    with get_conn() as conn:
                        insert_alert(
                            conn,
                            payload["vehicle_id"],
                            "Collision Risk",
                            f"Vehicle {payload['vehicle_id']} detected {payload.get('object_class', 'object')} "
                            f"at {payload.get('object_distance_m', 0):.1f}m, speed {payload.get('object_speed_kmh', 0):.0f} km/h",
                            payload.get("timestamp"),
                        )

            elif topic == topic_driving:
                driving_buf.append(payload)

        except Exception as e:
            logger.exception("Process message error: %s", e)

        # Batch write
        if len(telemetry_buf) >= BATCH_SIZE or len(perception_buf) >= BATCH_SIZE or len(driving_buf) >= BATCH_SIZE:
            try:
                with get_conn() as conn:
                    if telemetry_buf:
                        insert_vehicle_telemetry(conn, telemetry_buf)
                        telemetry_buf = []
                    if perception_buf:
                        insert_perception_events(conn, perception_buf)
                        perception_buf = []
                    if driving_buf:
                        insert_driving_events(conn, driving_buf)
                        driving_buf = []
            except Exception as e:
                logger.exception("Batch write error: %s", e)

    # Flush remaining
    try:
        with get_conn() as conn:
            if telemetry_buf:
                insert_vehicle_telemetry(conn, telemetry_buf)
            if perception_buf:
                insert_perception_events(conn, perception_buf)
            if driving_buf:
                insert_driving_events(conn, driving_buf)
    except Exception as e:
        logger.exception("Flush write error: %s", e)

    consumer.close()
    logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()
    sys.exit(0)
