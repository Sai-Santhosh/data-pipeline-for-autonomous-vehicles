# Pipeline: Real Data (OpenSky) — Verification

## OpenSky is real data, fully wired

1. **Ingestion**  
   `src/ingestion/opensky_source.py` fetches **live** state vectors from `https://opensky-network.org/api/states/all` (no simulation). Each aircraft is mapped to the same `vehicle_telemetry` schema (vehicle_id, timestamp, lat, lon, speed_kmh, etc.).

2. **Producer**  
   `run_producer_opensky()` in `src/ingestion/producer.py` uses `stream_opensky_to_kafka()` and sends every record to Kafka topics `vehicle_telemetry` and `perception_events`. Use:  
   `python scripts/run_producer.py live --source opensky`

3. **Consumer**  
   `src/processing/consumer.py` consumes those same topics and writes to TimescaleDB (same code path for simulation and OpenSky). No branch by source; payload shape is identical.

4. **Storage**  
   `src/storage/db.py` `insert_vehicle_telemetry()` and `insert_perception_events()` accept the OpenSky-mapped records (same keys as simulation). Invalid `ON CONFLICT` clauses were removed so inserts always succeed.

5. **Dashboard & metrics**  
   `distinct_vehicle_ids()` and other queries work for any vehicle_id (OpenSky uses 1–9999). Speed gauge supports high values (e.g. aircraft). All metrics (alerts, interventions, perception) work with real data.

## How to generate results (when pipeline runs)

With Docker (Kafka + TimescaleDB) and topics up:

```bash
# Terminal 1: consumer
python scripts/run_consumer.py

# Terminal 2: OpenSky producer (real data), then export
python scripts/run_producer.py live --source opensky
# Let it run 20–30 seconds (2–3 polls), then Ctrl+C

# Wait a few seconds for consumer to flush, then:
python scripts/export_results.py
```

Or use the helper (starts consumer in background, runs OpenSky for 3 polls, exports):

```bash
python scripts/run_pipeline_and_export.py --with-consumer
```

Exported files in `results/`:  
`latest_telemetry.csv`, `perception_summary_24h.csv`, `miles_per_intervention_sample.csv`, `alerts_sample.csv`, `interventions_per_vehicle_sample.csv`.
