#!/usr/bin/env python3
"""
Run the Kafka producer.
  Simulation (default):   python scripts/run_producer.py [live|50]
  Real data (OpenSky):    python scripts/run_producer.py live --source opensky
  Waymo-style replay:     python scripts/run_producer.py --source waymo_replay [path.csv]
                          (Waymo has no live API; replays from CSV/JSONL.)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingestion.producer import run_producer, run_producer_opensky, run_producer_waymo_replay

if __name__ == "__main__":
    source = "simulation"
    if "--source" in sys.argv:
        i = sys.argv.index("--source")
        if i + 1 < len(sys.argv):
            source = sys.argv[i + 1].lower()
    args = [a for a in sys.argv[1:] if a not in ("--source", "--loop") and not (a.startswith("--") and "source" not in a and a != "--loop")]
    mode = args[0] if args else "live"
    if source == "opensky":
        run_producer_opensky(mode=mode)
    elif source == "waymo_replay":
        file_path = args[1] if len(args) > 1 else None
        run_producer_waymo_replay(file_path=file_path, loop="--loop" in sys.argv)
    else:
        run_producer(mode=mode)
