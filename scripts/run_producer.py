#!/usr/bin/env python3
"""Run the Kafka producer (live or N records)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingestion.producer import run_producer

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    run_producer(mode=mode)
