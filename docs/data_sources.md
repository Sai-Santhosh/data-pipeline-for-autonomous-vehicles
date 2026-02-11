# Data sources

## Overview

The pipeline supports three ingestion modes:

| Source | Type | Description |
|--------|------|-------------|
| **Simulation** | Synthetic | Python-generated fleet telemetry (default). |
| **OpenSky** | Real API | Live aircraft positions → mapped to telemetry. |
| **Waymo replay** | File replay | CSV/JSONL replay (Waymo has no live API). |

---

## 1. Simulation (default)

- **Location:** `src/ingestion/producer.py`
- **What:** Python-generated vehicle telemetry, perception events, and driving events.
- **Details:** 10 vehicles, California locations (Palo Alto, San Francisco, LA), random speeds/battery/trips. Perception events use random object class, distance, speed. Driving events (intervention, disengagement) are randomly emitted.
- **No external API or files.**

---

## 2. OpenSky Network (real data)

- **Location:** `src/ingestion/opensky_source.py`
- **API:** [OpenSky Network REST API](https://openskynetwork.github.io/opensky-api/rest.html) — real-time aircraft state vectors (position, velocity, heading, etc.).
- **No API key** required for anonymous use. Rate limits: 400 API credits/day, 10-second time resolution. Using a bounding box reduces credit usage.
- **Mapping:** Each aircraft is treated as a “vehicle”: ICAO24 → `vehicle_id`, latitude/longitude, velocity (m/s → km/h), timestamp. Fields like battery and trip are set to placeholders (e.g. 100%, origin/destination from callsign or country).
- **Perception:** Nearby aircraft (within ~50 km) from the same poll are emitted as perception events (object_class = `"aircraft"`).

### Config (`config/settings.yaml`)

```yaml
opensky:
  api_url: "https://opensky-network.org/api/states/all"
  bbox: null   # optional [lamin, lomin, lamax, lomax] e.g. [37.0, -123.0, 38.0, -121.0]
  poll_interval_sec: 10
  max_vehicles: 20
```

### Run

```bash
python scripts/run_producer.py live --source opensky
```

---

---

## 3. Waymo-style replay (file → Kafka)

- **Location:** `src/ingestion/waymo_replay.py`, sample: `data/waymo_sample.csv`
- **Why:** Waymo Open Dataset does **not** offer a live or streaming API. Data is batch (TFRecord on Google Cloud Storage). This replay lets you feed the pipeline from a CSV or JSONL file that matches our telemetry schema.
- **Format:** CSV or JSONL with columns: `vehicle_id`, `timestamp`, `latitude`, `longitude`, `current_speed_kmh`, and optionally `speed_limit_violation`, `battery_level_pct`, `remaining_range_km`, `start_location`, `destination`, etc. Missing columns get defaults.
- **Config:** `config/settings.yaml` → `waymo_replay.telemetry_path`, `perception_path`, `speed_factor`, `loop`.

### Run

```bash
# Replay default sample (data/waymo_sample.csv)
python scripts/run_producer.py --source waymo_replay

# Replay a specific file
python scripts/run_producer.py --source waymo_replay path/to/telemetry.csv

# Replay in a loop
python scripts/run_producer.py --source waymo_replay --loop
```

### Using real Waymo Open Dataset

1. Download from [Waymo Open Dataset](https://waymo.com/open/download) (Motion, Perception, or End-to-End).
2. Export to CSV/JSONL with columns matching our schema (e.g. one row per vehicle per timestamp: vehicle_id, timestamp, lat, lon, speed_kmh, …).
3. Run replay: `python scripts/run_producer.py --source waymo_replay your_export.csv`.

---

## Other possible sources (not implemented)

- **GTFS Realtime:** Many transit agencies expose vehicle positions via GTFS Realtime (Protocol Buffer). Would require a library (e.g. `gtfs-realtime-bindings`) and feed URL; some agencies require an API key (e.g. LA Metro via Swiftly).
