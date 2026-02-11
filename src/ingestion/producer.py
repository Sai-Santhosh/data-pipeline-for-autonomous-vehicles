"""
Fleet telemetry and perception event producer for Self-Driving metrics.

Streams vehicle_telemetry, perception_events, and driving_events to Kafka
for real-time pipeline consumption. Aligned with fleet data mining and
Self-Driving performance measurement use cases.
"""
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Project root for config
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config
from src.logging_config import setup_logging, LOG_FILE

try:
    from kafka import KafkaProducer
except ImportError:
    raise ImportError("Install kafka-python: pip install kafka-python")

setup_logging(log_file=True, console=True)
logger = logging.getLogger(__name__)

# California locations for realistic fleet simulation
LOCATIONS = {
    "Palo Alto": [
        {"name": "Tesla HQ", "lat": 37.3947, "lon": -122.1503},
        {"name": "Stanford University", "lat": 37.4275, "lon": -122.1697},
        {"name": "University Ave", "lat": 37.4419, "lon": -122.1430},
    ],
    "San Francisco": [
        {"name": "Golden Gate Bridge", "lat": 37.8199, "lon": -122.4783},
        {"name": "Fisherman's Wharf", "lat": 37.8080, "lon": -122.4177},
        {"name": "SOMA", "lat": 37.7749, "lon": -122.4194},
    ],
    "Los Angeles": [
        {"name": "LAX", "lat": 33.9416, "lon": -118.4085},
        {"name": "Santa Monica Pier", "lat": 34.0100, "lon": -118.4957},
        {"name": "Downtown LA", "lat": 34.0522, "lon": -118.2437},
    ],
}

CITIES = {
    "Palo Alto": {"lat_min": 37.35, "lat_max": 37.50, "lon_min": -122.25, "lon_max": -122.10},
    "San Francisco": {"lat_min": 37.70, "lat_max": 37.85, "lon_min": -122.55, "lon_max": -122.35},
    "Los Angeles": {"lat_min": 33.90, "lat_max": 34.20, "lon_min": -118.60, "lon_max": -118.20},
}

OBJECT_CLASSES = ["car", "pedestrian", "cyclist", "truck", "motorcycle", "bus"]


def _ts_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _move_in_city(lat: float, lon: float, speed_kmh: float, direction: str, city: str) -> tuple[float, float]:
    delta = speed_kmh * 0.00001
    if direction == "straight":
        lat += delta
    elif direction == "left":
        lon -= delta
    else:
        lon += delta
    b = CITIES[city]
    lat = max(b["lat_min"], min(b["lat_max"], lat))
    lon = max(b["lon_min"], min(b["lon_max"], lon))
    return round(lat, 6), round(lon, 6)


def build_vehicle_telemetry(vehicle_id: int, state: dict) -> dict:
    """Emit one vehicle telemetry record (speed, location, battery, autonomy state)."""
    speed = max(20, min(90, state.get("speed", 50) + random.uniform(-4, 4)))
    state["speed"] = speed
    speed_violation = speed > 65
    drain = 0.04 if speed <= 60 else 0.08
    battery = max(0, state.get("battery", 80) - drain)
    state["battery"] = battery
    city = state.get("city", "Palo Alto")
    direction = random.choice(["left", "right", "straight"])
    lat, lon = _move_in_city(
        state["lat"], state["lon"], speed, direction, city
    )
    state["lat"], state["lon"] = lat, lon
    return {
        "vehicle_id": vehicle_id,
        "timestamp": _ts_utc(),
        "current_speed_kmh": round(speed, 2),
        "speed_limit_violation": speed_violation,
        "latitude": lat,
        "longitude": lon,
        "battery_level_pct": round(battery, 2),
        "remaining_range_km": round(battery * 5.2, 2),
        "autopilot_engaged": state.get("autopilot_engaged", True),
        "odometer_km": state.get("odometer_km", 0) + speed * (1 / 3600),
        "start_location": state.get("start_location", {}).get("name", "Unknown"),
        "destination": state.get("destination", {}).get("name", "Unknown"),
    }


def build_perception_event(vehicle_id: int) -> dict:
    """Emit one perception/detection event (object class, distance, speed) for Self-Driving metrics."""
    return {
        "vehicle_id": vehicle_id,
        "timestamp": _ts_utc(),
        "object_class": random.choice(OBJECT_CLASSES),
        "object_distance_m": round(random.uniform(2, 120), 2),
        "object_speed_kmh": round(random.uniform(0, 80), 2),
        "object_relative_direction": random.choice(["left", "right", "ahead", "rear"]),
        "confidence": round(random.uniform(0.85, 1.0), 3),
    }


def build_driving_event(vehicle_id: int, state: dict) -> dict | None:
    """Emit driving events: intervention, disengagement, near-miss (for Self-Driving metrics)."""
    # Simulate occasional interventions / disengagements
    if random.random() < 0.02:
        event_type = random.choice(["intervention", "disengagement", "lane_change", "hard_brake"])
        return {
            "vehicle_id": vehicle_id,
            "timestamp": _ts_utc(),
            "event_type": event_type,
            "event_detail": f"Driver {event_type} at speed {state.get('speed', 0):.0f} km/h",
            "latitude": state.get("lat"),
            "longitude": state.get("lon"),
        }
    return None


def run_producer(mode: str = "live", records_per_vehicle: int = 10) -> None:
    """Run the Kafka producer. mode: 'live' for continuous, or integer for N records per vehicle."""
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    topic_telemetry = cfg["kafka"]["topics"]["vehicle_telemetry"]
    topic_perception = cfg["kafka"]["topics"]["perception_events"]
    topic_driving = cfg["kafka"]["topics"]["driving_events"]

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    num_vehicles = 10
    vehicle_states = {}
    for vid in range(1, num_vehicles + 1):
        city = random.choice(list(CITIES.keys()))
        locs = LOCATIONS[city]
        start = random.choice(locs)
        dest = random.choice([l for l in locs if l != start])
        vehicle_states[vid] = {
            "speed": random.uniform(35, 65),
            "battery": random.uniform(70, 95),
            "lat": start["lat"],
            "lon": start["lon"],
            "city": city,
            "start_location": start,
            "destination": dest,
            "autopilot_engaged": True,
            "odometer_km": random.uniform(0, 5000),
        }

    if mode == "live":
        logger.info("Producer (simulation): streaming. Log file: %s", LOG_FILE)
        batch_num = 0
        try:
            while True:
                batch_num += 1
                for vid in range(1, num_vehicles + 1):
                    state = vehicle_states[vid]
                    producer.send(topic_telemetry, build_vehicle_telemetry(vid, state))
                    producer.send(topic_perception, build_perception_event(vid))
                    ev = build_driving_event(vid, state)
                    if ev:
                        producer.send(topic_driving, ev)
                if batch_num == 1 or batch_num % 10 == 0:
                    logger.info("Producer sent batch %s (%s telemetry + perception)", batch_num, num_vehicles)
                time.sleep(1)
        except KeyboardInterrupt:
            producer.flush()
            producer.close()
            logger.info("Producer stopped after %s batches", batch_num)
            return
    else:
        n = int(mode) if mode.isdigit() else records_per_vehicle
        for i in range(n):
            for vid in range(1, num_vehicles + 1):
                state = vehicle_states[vid]
                producer.send(topic_telemetry, build_vehicle_telemetry(vid, state))
                producer.send(topic_perception, build_perception_event(vid))
                ev = build_driving_event(vid, state)
                if ev:
                    producer.send(topic_driving, ev)
            if (i + 1) % 5 == 0 or i == 0:
                logger.info("Producer batch %s/%s sent", i + 1, n)
            time.sleep(0.5)
        producer.flush()
        producer.close()
        logger.info("Produced %s telemetry+perception records (simulation)", n * num_vehicles)


def run_producer_opensky(mode: str = "live", max_polls: int | None = None) -> None:
    """Run producer using real data from OpenSky Network API (aircraft positions → fleet telemetry)."""
    cfg = load_config()
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    topic_telemetry = cfg["kafka"]["topics"]["vehicle_telemetry"]
    topic_perception = cfg["kafka"]["topics"]["perception_events"]
    opensky_cfg = cfg.get("opensky", {})
    poll_interval = opensky_cfg.get("poll_interval_sec", 10)
    max_vehicles = opensky_cfg.get("max_vehicles", 20)
    bbox = opensky_cfg.get("bbox")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    def send_telemetry(record):
        producer.send(topic_telemetry, record)

    def send_perception(record):
        producer.send(topic_perception, record)

    from src.ingestion.opensky_source import stream_opensky_to_kafka
    if max_polls:
        logger.info("Producer (OpenSky real data): running %s polls then exiting. Log file: %s", max_polls, LOG_FILE)
    else:
        logger.info("Producer (OpenSky real data): streaming (Ctrl+C to stop). Log file: %s", LOG_FILE)
    try:
        stream_opensky_to_kafka(
            send_telemetry_fn=send_telemetry,
            send_perception_fn=send_perception,
            poll_interval_sec=poll_interval,
            max_vehicles=max_vehicles,
            bbox=bbox,
            max_polls=max_polls,
        )
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


def run_producer_waymo_replay(file_path: str | None = None, loop: bool = False) -> None:
    """Run producer using Waymo-style replay from CSV/JSONL file (no live Waymo API)."""
    cfg = load_config()
    waymo_cfg = cfg.get("waymo_replay", {})
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    path = file_path or waymo_cfg.get("telemetry_path", "data/waymo_sample.csv")
    if not Path(path).is_absolute():
        path = root / path
    perception_path = waymo_cfg.get("perception_path")
    if perception_path and not Path(perception_path).is_absolute():
        perception_path = root / perception_path
    speed_factor = float(waymo_cfg.get("speed_factor", 1.0))
    loop = loop or waymo_cfg.get("loop", False)
    from src.ingestion.waymo_replay import run_waymo_replay
    logger.info("Producer (Waymo replay): sending from %s. Log file: %s", path, LOG_FILE)
    run_waymo_replay(
        telemetry_path=path,
        perception_path=perception_path if perception_path and Path(perception_path).exists() else None,
        speed_factor=speed_factor,
        loop=loop,
    )


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--source" and not a.startswith("--")]
    source = "simulation"
    if "--source" in sys.argv:
        i = sys.argv.index("--source")
        if i + 1 < len(sys.argv):
            source = sys.argv[i + 1].lower()
    mode = args[0] if args else "live"
    if source == "opensky":
        run_producer_opensky(mode=mode)
    elif source == "waymo_replay":
        file_arg = args[1] if len(args) > 1 else None
        run_producer_waymo_replay(file_path=file_arg, loop="--loop" in sys.argv)
    else:
        run_producer(mode=mode)
