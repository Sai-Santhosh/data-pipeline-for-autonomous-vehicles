# Results

This folder stores pipeline outputs and analysis results.

- **Real data (OpenSky)**: When you run the pipeline with `--source opensky`, telemetry and alerts come from the OpenSky Network API (real aircraft positions). See `PIPELINE_REAL_DATA.md` for how it’s wired.
- **CSV exports**: After running the pipeline (consumer + producer), run from repo root:
  - `python scripts/export_results.py` — writes:
    - `latest_telemetry.csv`, `perception_summary_24h.csv`, `miles_per_intervention_sample.csv`, `alerts_sample.csv`, `interventions_per_vehicle_sample.csv`
  - Or: `python scripts/run_pipeline_and_export.py --with-consumer` (runs OpenSky for 3 polls then exports).
- **Sample files**: `latest_telemetry_sample.csv` and `alerts_sample_from_opensky.csv` show the expected format when using OpenSky (real data).
- **Notebooks**: Run notebooks in `../notebooks/` and export figures/tables here if needed.
