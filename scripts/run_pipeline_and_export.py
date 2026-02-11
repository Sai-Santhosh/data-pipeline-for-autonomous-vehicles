#!/usr/bin/env python3
"""
Run the pipeline with real OpenSky data and export results to results/.

Prerequisites: Docker (Kafka + TimescaleDB) up, topics created.
  docker-compose up -d
  python scripts/create_topics.ps1  # or create_topics.sh

Usage:
  Terminal 1: python scripts/run_consumer.py
  Terminal 2: python scripts/run_pipeline_and_export.py

Or run consumer in background (this script starts it):
  python scripts/run_pipeline_and_export.py --with-consumer

This script will:
  1) Optionally start consumer in background (--with-consumer)
  2) Run OpenSky producer for --max-polls (default 3) to ingest real data
  3) Wait for writes to complete
  4) Export CSVs to results/
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    ap = argparse.ArgumentParser(description="Run pipeline (OpenSky) and export results")
    ap.add_argument("--with-consumer", action="store_true", help="Start consumer in background")
    ap.add_argument("--max-polls", type=int, default=3, help="OpenSky polls (default 3)")
    ap.add_argument("--drain-sec", type=int, default=8, help="Seconds to wait for consumer drain")
    args = ap.parse_args()

    consumer_proc = None
    if args.with_consumer:
        print("Starting consumer in background...")
        consumer_proc = subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "run_consumer.py")],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        time.sleep(5)

    try:
        print("Running OpenSky producer (real data) for", args.max_polls, "polls...")
        from src.ingestion.producer import run_producer_opensky
        run_producer_opensky(mode="live", max_polls=args.max_polls)
        print("Waiting", args.drain_sec, "s for consumer to write...")
        time.sleep(args.drain_sec)

        print("Exporting results to results/...")
        subprocess.run([sys.executable, str(ROOT / "scripts" / "export_results.py")], cwd=ROOT, check=True)
        print("Done. Check results/ for CSVs.")
    finally:
        if consumer_proc:
            consumer_proc.terminate()
            consumer_proc.wait(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
