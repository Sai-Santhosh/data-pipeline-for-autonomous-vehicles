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
        from src.metrics.queries import (
            miles_per_intervention,
            alerts_summary,
            interventions_per_vehicle,
            latest_telemetry,
            perception_summary,
            intervention_rate_per_1000km,
            disengagement_rate_per_1000km,
            fleet_self_driving_summary,
            autopilot_engagement_rate,
        )
    except Exception as e:
        print("Import failed (install deps, set config):", e)
        return 1
    try:
        latest_telemetry().to_csv(RESULTS / "latest_telemetry.csv", index=False)
        print("Wrote results/latest_telemetry.csv")
    except Exception as e:
        print("latest_telemetry:", e)
    try:
        perception_summary(hours=24).to_csv(RESULTS / "perception_summary_24h.csv", index=False)
        print("Wrote results/perception_summary_24h.csv")
    except Exception as e:
        print("perception_summary:", e)
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
    try:
        intervention_rate_per_1000km(24).to_csv(RESULTS / "intervention_rate_per_1000km.csv", index=False)
        print("Wrote results/intervention_rate_per_1000km.csv")
    except Exception as e:
        print("intervention_rate_per_1000km:", e)
    try:
        disengagement_rate_per_1000km(24).to_csv(RESULTS / "disengagement_rate_per_1000km.csv", index=False)
        print("Wrote results/disengagement_rate_per_1000km.csv")
    except Exception as e:
        print("disengagement_rate_per_1000km:", e)
    try:
        fleet_self_driving_summary(24).to_csv(RESULTS / "fleet_self_driving_summary.csv", index=False)
        print("Wrote results/fleet_self_driving_summary.csv")
    except Exception as e:
        print("fleet_self_driving_summary:", e)
    try:
        autopilot_engagement_rate(24).to_csv(RESULTS / "autopilot_engagement_rate.csv", index=False)
        print("Wrote results/autopilot_engagement_rate.csv")
    except Exception as e:
        print("autopilot_engagement_rate:", e)
    return 0

if __name__ == "__main__":
    sys.exit(main())
