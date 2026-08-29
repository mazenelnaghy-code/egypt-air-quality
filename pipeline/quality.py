"""QUALITY — assertions that run against the built warehouse.

A pipeline without tests does not fail, it just quietly produces wrong
numbers, which is worse. Each check is a SQL query that must return zero
rows (or a single expected value). Failures stop the run, so bad data
never reaches the dashboard.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb

log = logging.getLogger(__name__)


@dataclass
class Check:
    name: str
    sql: str            # must return a single number
    expect_zero: bool = True
    severity: str = "error"   # "error" fails the run, "warn" only logs
    note: str = ""


CHECKS = [
    Check(
        "fact table is not empty",
        "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM fact_hourly_air_quality",
        note="An empty warehouse means extract or transform silently did nothing.",
    ),
    Check(
        "no null keys",
        """SELECT COUNT(*) FROM fact_hourly_air_quality
           WHERE city_id IS NULL OR observed_at IS NULL""",
        note="Primary key columns must never be null.",
    ),
    Check(
        "grain is unique",
        """SELECT COUNT(*) FROM (
               SELECT city_id, observed_at
               FROM fact_hourly_air_quality
               GROUP BY 1, 2 HAVING COUNT(*) > 1
           )""",
        note="One row per city per hour. Duplicates would double-count every average.",
    ),
    Check(
        "referential integrity",
        """SELECT COUNT(*) FROM fact_hourly_air_quality f
           LEFT JOIN dim_city c USING (city_id)
           WHERE c.city_id IS NULL""",
        note="Every fact row must point at a real city.",
    ),
    Check(
        "pm2_5 within plausible range",
        """SELECT COUNT(*) FROM fact_hourly_air_quality
           WHERE pm2_5 IS NOT NULL AND (pm2_5 < 0 OR pm2_5 > 2000)""",
        note="Negative or absurd concentrations mean an upstream change.",
    ),
    Check(
        "temperature within plausible range",
        """SELECT COUNT(*) FROM fact_hourly_air_quality
           WHERE temperature_c IS NOT NULL
             AND (temperature_c < -20 OR temperature_c > 60)""",
        note="Egypt is hot, but not that hot.",
    ),
    Check(
        "latest readings are observations, not forecasts",
        """SELECT COUNT(*) FROM mart_latest_city
           WHERE observed_at > (NOW() AT TIME ZONE 'UTC')""",
        note="The dashboard presents these as current readings, so a forecast "
             "hour appearing here is published as if it had been measured.",
    ),
    Check(
        "daily averages exclude forecast hours",
        """SELECT COUNT(*) FROM mart_daily_city
           WHERE date_key > (NOW() AT TIME ZONE 'UTC')::DATE""",
        note="A daily mean built partly from predictions is not a daily mean.",
    ),
    Check(
        "data is fresh",
        """SELECT CASE WHEN MAX(ingested_at) > NOW()::TIMESTAMP - INTERVAL 36 HOUR
                       THEN 0 ELSE 1 END
           FROM fact_hourly_air_quality""",
        note="If the newest ingest is over 36h old, the schedule has stopped running.",
    ),
    Check(
        "all cities reporting",
        f"""SELECT CASE WHEN COUNT(DISTINCT city_id) >= 6 THEN 0 ELSE 1 END
            FROM fact_hourly_air_quality
            WHERE observed_at > NOW()::TIMESTAMP - INTERVAL 48 HOUR""",
        severity="warn",
        note="A city dropping out is worth knowing about but not worth failing on.",
    ),
    Check(
        "pm2_5 mostly populated",
        """SELECT CASE WHEN
               SUM(CASE WHEN pm2_5 IS NULL THEN 1 ELSE 0 END) * 1.0
               / NULLIF(COUNT(*), 0) < 0.2
             THEN 0 ELSE 1 END
           FROM fact_hourly_air_quality""",
        severity="warn",
        note="More than 20% nulls in the headline metric suggests a source problem.",
    ),
]


def run(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Execute every check. Raises if an error-severity check fails."""
    results = []
    failed = []

    for check in CHECKS:
        value = con.execute(check.sql).fetchone()[0] or 0
        ok = (value == 0) if check.expect_zero else (value != 0)
        results.append(
            {"name": check.name, "passed": ok, "value": value,
             "severity": check.severity, "note": check.note}
        )
        symbol = "PASS" if ok else ("WARN" if check.severity == "warn" else "FAIL")
        log.info("%-5s %s", symbol, check.name)
        if not ok and check.severity == "error":
            failed.append(f"{check.name} ({check.note})")

    if failed:
        raise AssertionError(
            "data quality checks failed:\n  - " + "\n  - ".join(failed)
        )
    return results
