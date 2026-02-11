# Data

- **waymo_sample.csv** — Sample telemetry (schema-compatible with pipeline). Use with:
  `python scripts/run_producer.py --source waymo_replay`
- For **Waymo Open Dataset**: export Motion/Perception data to CSV with columns matching `vehicle_telemetry` (see docs/data_sources.md), then replay with `--source waymo_replay path/to/export.csv`.
