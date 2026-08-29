"""EXTRACT — pull hourly readings from Open-Meteo and land them as raw JSONL.

Design notes
------------
* Raw files are append-only and partitioned by the date the reading
  describes, not the date it was fetched. They are the source of truth:
  the warehouse can always be rebuilt from them.
* Nothing is cleaned or reshaped here. Extract stores what the API said,
  as close to verbatim as is practical. Cleaning happens downstream, so a
  bug in cleaning never costs us the original data.
* Each row carries `ingested_at`, which is how the transform layer decides
  which version of a duplicated reading wins.
"""
from __future__ import annotations

import gzip
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from .config import (
    AIR_QUALITY_URL,
    AIR_VARS,
    CITIES,
    KEEP_PLAIN_DAYS,
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


def fetch_city(city: dict, ingested_at: str, past_days: int = PAST_DAYS) -> list[dict]:
    """Fetch air quality and weather for one city and merge on timestamp."""
    common = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "past_days": past_days,
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


def _write_partitions(rows: list[dict], raw_dir: Path) -> dict[str, int]:
    """Append rows to one file per observation date. Returns rows per file.

    Partitioning on the date a reading describes rather than the date it was
    fetched means a file's name tells you what is inside it, so "what did
    Cairo look like on the 21st?" is one file rather than a scan of every
    partition. It also keeps a backfill proportionate: 30 days of history
    lands as 30 ordinary files instead of one outsized file whose name claims
    it holds a single day.

    Files stay append-only. A given date is written by every run whose window
    covers it — about four days' worth — and is never touched again, so
    partitions become effectively immutable a few days after the fact. The
    repeated writes are the same overlapping readings the transform layer
    already de-duplicates on (city_id, observed_at).
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    by_date: dict[str, list[dict]] = {}
    for row in rows:
        # observed_at is "YYYY-MM-DDTHH:MM" as the API returns it, in UTC.
        by_date.setdefault(row["observed_at"][:10], []).append(row)

    for date, day_rows in sorted(by_date.items()):
        with (raw_dir / f"{date}.jsonl").open("a", encoding="utf-8") as handle:
            for row in day_rows:
                handle.write(json.dumps(row, separators=(",", ":")) + "\n")

    return {date: len(day_rows) for date, day_rows in sorted(by_date.items())}


def _write_gz(path: Path, text: str) -> None:
    """Write gzip whose bytes depend only on the content.

    GzipFile stamps a modification time and the name of the file it was opened
    with into the header. Both would make the output differ between runs that
    produced identical data — the archive of a finished day would keep churning
    in git, and the reproducibility the rest of the pipeline goes to some
    trouble for would stop at the raw layer. Writing through a file object with
    the timestamp zeroed and the name blank removes both.

    Written to a temporary file and renamed, so an interrupted run cannot leave
    a half-written archive where a complete partition used to be.
    """
    tmp = path.parent / (path.name + ".tmp")
    with open(tmp, "wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9,
                           mtime=0, filename="") as handle:
            handle.write(text.encode("utf-8"))
    tmp.replace(path)


def compact_partitions(
    raw_dir: Path = RAW_DIR, keep_plain_days: int = KEEP_PLAIN_DAYS
) -> dict[str, tuple[int, int]]:
    """Gzip partitions no future run will append to. Returns {date: (before, after)}.

    Safe to run at any time and safe to run twice: it only touches dates that
    have fallen out of the extract window, and a partition already compressed
    is left alone. A backfill reaching back into compressed history writes a
    plain file alongside the archive; the next compaction merges the two rather
    than letting the day end up split across both.
    """
    # keep_plain_days counts the days left uncompressed, today included, so
    # keep_plain_days=4 leaves today and the three days before it plain.
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=keep_plain_days - 1)
    results: dict[str, tuple[int, int]] = {}

    for path in sorted(raw_dir.glob("*.jsonl")):
        try:
            day = date.fromisoformat(path.stem)
        except ValueError:
            continue                      # not a date-named partition
        if day >= cutoff:
            continue                      # still inside the write window

        archive = path.parent / (path.name + ".gz")
        lines = path.read_text(encoding="utf-8").splitlines()
        before = path.stat().st_size

        if archive.exists():
            with gzip.open(archive, "rt", encoding="utf-8") as handle:
                lines = handle.read().splitlines() + lines
            before += archive.stat().st_size

        _write_gz(archive, "\n".join(lines) + "\n")
        path.unlink()
        results[path.stem] = (before, archive.stat().st_size)

    if results:
        before = sum(b for b, _ in results.values())
        after = sum(a for _, a in results.values())
        log.info("compacted %s partitions, %.0f KB -> %.0f KB (%.1fx)",
                 len(results), before / 1024, after / 1024, before / max(after, 1))
    return results


def run(past_days: int = PAST_DAYS) -> int:
    """Fetch every city and append to the raw partitions it covers.

    A backfill is the same call with a wider `past_days`; it simply touches
    more partitions. The transform layer keys on (city, observed_at) and keeps
    the most recently ingested version, so re-fetching a day refreshes it with
    the model's corrected values rather than duplicating it.

    Returns the number of rows written.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    rows: list[dict] = []
    failures: list[str] = []

    for city in CITIES:
        try:
            rows.extend(fetch_city(city, ingested_at, past_days))
        except Exception as exc:  # noqa: BLE001
            # One dead city should not sink the whole run.
            log.error("skipping %s: %s", city["city_id"], exc)
            failures.append(city["city_id"])

    if not rows:
        raise RuntimeError("extract produced no rows at all - aborting")

    if failures:
        log.warning("failed cities: %s", ", ".join(failures))

    written = _write_partitions(rows, RAW_DIR)

    span = f"{min(written)} .. {max(written)}" if len(written) > 1 else next(iter(written))
    log.info("wrote %s rows across %s partitions (%s)", len(rows), len(written), span)
    return len(rows)
