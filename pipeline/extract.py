"""EXTRACT — pull hourly readings from Open-Meteo and land them as raw JSONL.

Design notes
------------
* Raw files are append-only and partitioned by ingest date. They are the
  source of truth: the warehouse can always be rebuilt from them.
* Nothing is cleaned or reshaped here. Extract stores what the API said,
  as close to verbatim as is practical. Cleaning happens downstream, so a
  bug in cleaning never costs us the original data.
* Each row carries `ingested_at`, which is how the transform layer decides
  which version of a duplicated reading wins.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import requests

from .config import (
    AIR_QUALITY_URL,
    AIR_VARS,
    CITIES,
    PAST_DAYS,
    RAW_DIR,
    WEATHER_URL,
    WEATHER_VARS,
)

log = logging.getLogger(__name__)

TIMEOUT = 30
RETRIES = 3
BACKOFF = 4  # seconds, doubled each retry


def _get(url: str, params: dict) -> dict:
    """GET with retries. Network calls fail; a pipeline that assumes
    otherwise breaks at 3am and nobody knows why."""
    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - we genuinely want any failure
            last_error = exc
            wait = BACKOFF * (2 ** (attempt - 1))
            log.warning("request failed (attempt %s/%s): %s", attempt, RETRIES, exc)
            if attempt < RETRIES:
                time.sleep(wait)
    raise RuntimeError(f"giving up on {url} after {RETRIES} attempts") from last_error


def _to_rows(city: dict, payload: dict, variables: list[str], ingested_at: str) -> dict:
    """Open-Meteo returns column arrays that line up by index with `time`.
    Flip that into one dict per hour, keyed by timestamp."""
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    out: dict[str, dict] = {}

    for i, observed_at in enumerate(times):
        row = {
            "city_id": city["city_id"],
            "observed_at": observed_at,
            "ingested_at": ingested_at,
        }
        for var in variables:
            series = hourly.get(var) or []
            row[var] = series[i] if i < len(series) else None
        out[observed_at] = row
    return out


def fetch_city(city: dict, ingested_at: str) -> list[dict]:
    """Fetch air quality and weather for one city and merge on timestamp."""
    common = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "past_days": PAST_DAYS,
        "forecast_days": 1,
        "timezone": "UTC",
    }

    air = _get(AIR_QUALITY_URL, {**common, "hourly": ",".join(AIR_VARS)})
    weather = _get(WEATHER_URL, {**common, "hourly": ",".join(WEATHER_VARS)})

    air_rows = _to_rows(city, air, AIR_VARS, ingested_at)
    weather_rows = _to_rows(city, weather, WEATHER_VARS, ingested_at)

    # Left join on the air-quality timestamps: a reading without pollution
    # data is not useful to us, but missing weather is tolerable.
    merged = []
    for observed_at, row in air_rows.items():
        extra = weather_rows.get(observed_at, {})
        for var in WEATHER_VARS:
            row[var] = extra.get(var)
        merged.append(row)

    log.info("%-12s %s rows", city["city_id"], len(merged))
    return merged


def run() -> int:
    """Fetch every city and append to today's raw partition.

    Returns the number of rows written.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    partition = RAW_DIR / f"{ingested_at[:10]}.jsonl"

    rows: list[dict] = []
    failures: list[str] = []

    for city in CITIES:
        try:
            rows.extend(fetch_city(city, ingested_at))
        except Exception as exc:  # noqa: BLE001
            # One dead city should not sink the whole run.
            log.error("skipping %s: %s", city["city_id"], exc)
            failures.append(city["city_id"])

    if not rows:
        raise RuntimeError("extract produced no rows at all - aborting")

    if failures:
        log.warning("failed cities: %s", ", ".join(failures))

    with partition.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    log.info("wrote %s rows to %s", len(rows), partition.name)
    return len(rows)
