#!/usr/bin/env python3
"""Run the Kafka consumer (writes to TimescaleDB)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.processing.consumer import main

if __name__ == "__main__":
    main()
