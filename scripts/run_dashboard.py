#!/usr/bin/env python3
"""Launch Streamlit dashboard. Run from repo root: python scripts/run_dashboard.py"""
import sys
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[1]
dashboard = root / "src" / "visualization" / "dashboard.py"
if not dashboard.exists():
    print("Dashboard not found:", dashboard)
    sys.exit(1)
subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard), "--server.port", "8501"], cwd=root)
