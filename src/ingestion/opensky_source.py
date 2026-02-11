"""
Real data source: OpenSky Network API.
Real-time aircraft position/velocity data mapped to fleet telemetry schema.
No API key required for anonymous access (rate limits apply: 400 credits/day, 10s resolution).
Docs: https://openskynetwork.github.io/opensky-api/rest.html
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Project root for config
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import load_config

logger = logging.getLogger(__name__)

# OpenSky state vector indices (see API docs)
IDX_ICAO24 = 0
IDX_CALLSIGN = 1
IDX_ORIGIN_COUNTRY = 2
IDX_TIME_POSITION = 3
IDX_LAST_CONTACT = 4
IDX_LONGITUDE = 5
IDX_LATITUDE = 6
IDX_BARO_ALTITUDE = 7
IDX_ON_GROUND = 8
IDX_VELOCITY = 9
IDX_TRUE_TRACK = 10


def _vehicle_id_from_icao24(icao24: str) -> int:
    """Stable integer vehicle_id from ICAO24 hex string (1–9999)."""
    if not icao24:
        return 1
    n = int(icao24, 16) if isinstance(icao24, str) else hash(icao24)
    return (n & 0x7FFF_FFFF) % 9999 + 1


def _ts_from_unix(unix_sec: int | None) -> str:
    if unix_sec is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.fromtimestamp(unix_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fetch_opensky_states(bbox: list[float] | None = None) -> list[list]:
    """
    Fetch current state vectors from OpenSky.
    bbox: [lamin, lomin, lamax, lomax] or None for worldwide (uses more API credits).
    """
    url = load_config().get("opensky", {}).get("api_url", "https://opensky-network.org/api/states/all")
    params = {}
    if bbox and len(bbox) >= 4:
        params = {"lamin": bbox[0], "lomin": bbox[1], "lamax": bbox[2], "lomax": bbox[3]}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("states") or []
    except Exception as e:
        logger.warning("OpenSky fetch failed: %s", e)
        return []


def opensky_state_to_telemetry(state: list) -> dict | None:
    """Map one OpenSky state vector to our vehicle_telemetry schema."""
    if not state or len(state) <= IDX_VELOCITY:
        return None
    icao24 = state[IDX_ICAO24] or "unknown"
    lat = state[IDX_LATITUDE]
    lon = state[IDX_LONGITUDE]
    if lat is None or lon is None:
        return None
    velocity_ms = state[IDX_VELOCITY]
    speed_kmh = (velocity_ms * 3.6) if velocity_ms is not None else 0.0
    time_pos = state[IDX_TIME_POSITION]
    callsign = (state[IDX_CALLSIGN] or "").strip() or "N/A"
    origin = (state[IDX_ORIGIN_COUNTRY] or "N/A")

    return {
        "vehicle_id": _vehicle_id_from_icao24(icao24),
        "timestamp": _ts_from_unix(time_pos),
        "current_speed_kmh": round(speed_kmh, 2),
        "speed_limit_violation": speed_kmh > 65,
        "latitude": round(float(lat), 6),
        "longitude": round(float(lon), 6),
        "battery_level_pct": 100.0,   # N/A for aircraft; placeholder
        "remaining_range_km": 500.0,
        "autopilot_engaged": True,
        "odometer_km": 0.0,
        "start_location": origin,
        "destination": callsign or origin,
    }


def opensky_state_to_perception(state: list, other_states: list[list]) -> list[dict]:
    """
    For one state, generate perception events for "nearby" aircraft (as detected objects).
    other_states: other state vectors from same poll (excluding current).
    """
    out = []
    if not state or len(state) <= IDX_LATITUDE:
        return out
    icao24 = state[IDX_ICAO24]
    lat = state[IDX_LATITUDE]
    lon = state[IDX_LONGITUDE]
    velocity_ms = state[IDX_VELOCITY] or 0
    speed_kmh = velocity_ms * 3.6
    time_pos = state[IDX_TIME_POSITION]
    vehicle_id = _vehicle_id_from_icao24(icao24)

    for other in other_states:
        if not other or other[IDX_ICAO24] == icao24:
            continue
        o_lat = other[IDX_LATITUDE]
        o_lon = other[IDX_LONGITUDE]
        if o_lat is None or o_lon is None:
            continue
        # Approximate distance (haversine simplified for small distances)
        dlat = (o_lat - lat) * 111_000  # m
        dlon = (o_lon - lon) * 111_000 * max(0.7, abs(lat) / 90)
        dist_m = (dlat ** 2 + dlon ** 2) ** 0.5
        if dist_m > 50_000:  # only "nearby" in same general area
            continue
        o_vel = other[IDX_VELOCITY]
        o_speed_kmh = (o_vel * 3.6) if o_vel is not None else 0
        out.append({
            "vehicle_id": vehicle_id,
            "timestamp": _ts_from_unix(time_pos),
            "object_class": "aircraft",
            "object_distance_m": round(dist_m, 2),
            "object_speed_kmh": round(o_speed_kmh, 2),
            "object_relative_direction": "ahead",
            "confidence": 0.95,
        })
        if len(out) >= 3:
            break
    return out


def stream_opensky_to_kafka(
    send_telemetry_fn,
    send_perception_fn,
    poll_interval_sec: int = 10,
    max_vehicles: int = 20,
    bbox: list[float] | None = None,
    max_polls: int | None = None,
) -> None:
    """
    Poll OpenSky every poll_interval_sec and send telemetry (+ perception) to Kafka via callbacks.
    send_telemetry_fn(record), send_perception_fn(record) each send one record.
    If max_polls is set, stop after that many successful polls (for one-shot/CI).
    """
    polls_done = 0
    while True:
        states = fetch_opensky_states(bbox)
        if not states:
            logger.info("OpenSky: no states this poll")
            time.sleep(poll_interval_sec)
            continue
        to_emit = states[:max_vehicles]
        for state in to_emit:
            telemetry = opensky_state_to_telemetry(state)
            if telemetry:
                send_telemetry_fn(telemetry)
            for rec in opensky_state_to_perception(state, to_emit):
                send_perception_fn(rec)
        polls_done += 1
        logger.info("OpenSky: emitted %s vehicles (poll %s)", len(to_emit), polls_done)
        if max_polls is not None and polls_done >= max_polls:
            return
        time.sleep(poll_interval_sec)
