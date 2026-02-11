"""
Pipeline logging: console + optional log file.
Creates logs/ in project root and writes pipeline.log (and current run to console).
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # new_tesla/
LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level=logging.INFO,
    log_file=True,
    console=True,
):
    """Configure root logger: console and optionally logs/pipeline.log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers when called multiple times
    if root.handlers:
        return
    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FMT)
    if console:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(level)
        h.setFormatter(formatter)
        root.addHandler(h)
    if log_file:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    logging.getLogger("kafka").setLevel(logging.WARNING)  # reduce Kafka noise
    return root
