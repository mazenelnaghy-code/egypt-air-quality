"""TRANSFORM — build the warehouse from raw files by running SQL models.

The whole warehouse is rebuilt from raw on every run. That sounds wasteful
and at this data volume it costs about a second, in exchange for a property
worth far more: the pipeline is idempotent. Any run produces exactly the
same warehouse, so a failed or half-finished run is never a problem.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb

from .config import (
    CITIES,
    LOCAL_TIMEZONE,
    RAW_DIR,
    SQL_DIR,
    WAREHOUSE,
    WHO_PM25_GUIDELINE,
)

log = logging.getLogger(__name__)

# Executed in filename order; the numeric prefixes ARE the dependency order.
MODELS = ["10_staging.sql", "20_dimensions.sql", "30_facts.sql", "40_marts.sql"]


def run(
    raw_dir: Path | None = None,
    warehouse: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Build the warehouse from raw JSONL.

    The paths are arguments rather than constants so tests can build a
    throwaway warehouse from synthetic data. Nothing that writes to the
    real data/ directory belongs in a test run.
    """
    raw_dir = raw_dir or RAW_DIR
    warehouse = warehouse or WAREHOUSE

    if not any(raw_dir.glob("*.jsonl")):
        raise RuntimeError(f"no raw data in {raw_dir} - run extract first")

    # Cities live in Python config but need to reach SQL. Writing them to a
    # temp JSON file keeps dim_city defined in SQL like every other model.
    # aqi_grid is defaulted here rather than in SQL so the column always
    # exists, even if no city in config declares a shared grid cell.
    cities = [{"aqi_grid": c["city_id"], **c} for c in CITIES]
    cities_file = raw_dir.parent / "_cities.json"
    cities_file.write_text(json.dumps(cities), encoding="utf-8")

    warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))

    # Extract requests timezone=UTC, so every observed_at and ingested_at in
    # the warehouse is naive UTC. DuckDB's NOW()::TIMESTAMP otherwise resolves
    # to the machine's local wall clock, and comparing that against UTC data
    # shifts every "is this hour in the past?" test by the local UTC offset.
    # In Egypt (UTC+3) that let three hours of forecast into mart_latest_city
    # while a UTC CI runner saw none of it. Pin the session to UTC so the
    # warehouse is identical wherever it is built.
    con.execute("SET TimeZone = 'UTC'")

    con.execute(f"SET VARIABLE raw_glob = '{raw_dir.as_posix()}/*.jsonl'")
    con.execute(f"SET VARIABLE cities_json = '{cities_file.as_posix()}'")
    con.execute(f"SET VARIABLE who_pm25 = {WHO_PM25_GUIDELINE}")
    con.execute(f"SET VARIABLE local_tz = '{LOCAL_TIMEZONE}'")

    for model in MODELS:
        sql = (SQL_DIR / model).read_text(encoding="utf-8")
        log.info("running %s", model)
        con.execute(sql)

    rows = con.execute("SELECT COUNT(*) FROM fact_hourly_air_quality").fetchone()[0]
    log.info("warehouse built: %s fact rows", rows)
    return con
