"""Configuration package for the Fleet Data Pipeline."""
from pathlib import Path
import os
import yaml

_CONFIG_PATH = Path(__file__).resolve().parent / "settings.yaml"
_ROOT = _CONFIG_PATH.resolve().parents[1]


def load_config():
    """Load YAML config with env overrides. Loads .env from project root if present."""
    try:
        from dotenv import load_dotenv
        load_dotenv(_ROOT / ".env")
    except ImportError:
        pass
    with open(_CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    # Env overrides
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        cfg["kafka"]["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if os.getenv("TIMESCALE_HOST"):
        cfg["timescaledb"]["host"] = os.getenv("TIMESCALE_HOST")
    if os.getenv("TIMESCALE_PORT"):
        cfg["timescaledb"]["port"] = int(os.getenv("TIMESCALE_PORT"))
    if os.getenv("TIMESCALE_DB"):
        cfg["timescaledb"]["database"] = os.getenv("TIMESCALE_DB")
    if os.getenv("TIMESCALE_USER"):
        cfg["timescaledb"]["user"] = os.getenv("TIMESCALE_USER")
    if os.getenv("TIMESCALE_PASSWORD"):
        cfg["timescaledb"]["password"] = os.getenv("TIMESCALE_PASSWORD")
    return cfg
