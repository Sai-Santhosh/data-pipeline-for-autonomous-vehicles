#!/usr/bin/env python3
"""
Export sample metric results to CSV in results/.
Run from repo root after pipeline has been running: python scripts/export_results.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
RESULTS = ROOT / "results"

def main():
    RESULTS.mkdir(exist_ok=True)
    try:
        from src.metrics.queries import miles_per_intervention, alerts_summary, interventions_per_vehicle
    except Exception as e:
        print("Import failed (install deps, set config):", e)
        return 1
    try:
        mpi = miles_per_intervention(24)
        mpi.to_csv(RESULTS / "miles_per_intervention_sample.csv", index=False)
        print("Wrote results/miles_per_intervention_sample.csv")
    except Exception as e:
        print("miles_per_intervention:", e)
    try:
        alerts_summary(limit=100).to_csv(RESULTS / "alerts_sample.csv", index=False)
        print("Wrote results/alerts_sample.csv")
    except Exception as e:
        print("alerts_summary:", e)
    try:
        interventions_per_vehicle(hours=24).to_csv(RESULTS / "interventions_per_vehicle_sample.csv", index=False)
        print("Wrote results/interventions_per_vehicle_sample.csv")
    except Exception as e:
        print("interventions_per_vehicle:", e)
    return 0

if __name__ == "__main__":
    sys.exit(main())
