"""PUBLISH — export the marts and render the static dashboard.

Two audiences:
  * People, who get docs/index.html on GitHub Pages.
  * Machines, who get CSV and JSON files at stable URLs, so anyone can
    pull this data into pandas or a spreadsheet without asking permission.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import duckdb

from .config import ATTRIBUTION, DOCS_DIR, EXPORT_DIR

log = logging.getLogger(__name__)

EXPORTS = [
    "mart_daily_city",
    "mart_latest_city",
    "mart_hourly_profile",
    "mart_pipeline_health",
]


def _export_tables(con: duckdb.DuckDBPyConnection) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    for table in EXPORTS:
        csv_path = EXPORT_DIR / f"{table}.csv"
        con.execute(
            f"COPY (SELECT * FROM {table}) TO '{csv_path.as_posix()}' "
            "(HEADER, DELIMITER ',')"
        )
        log.info("exported %s", csv_path.name)


def _json_rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cursor = con.execute(sql)
    columns = [d[0] for d in cursor.description]
    return [
        {c: (v.isoformat() if hasattr(v, "isoformat") else v)
         for c, v in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def run(con: duckdb.DuckDBPyConnection, checks: list[dict]) -> None:
    _export_tables(con)

    latest = _json_rows(
        con, "SELECT * FROM mart_latest_city ORDER BY us_aqi DESC NULLS LAST"
    )
    daily = _json_rows(
        con,
        """SELECT city_name, date_key::VARCHAR AS date_key, avg_pm2_5, avg_us_aqi
           FROM mart_daily_city
           WHERE date_key >= CURRENT_DATE - 14
           ORDER BY date_key""",
    )
    profile = _json_rows(
        con,
        "SELECT city_name, hour_of_day, avg_pm2_5 FROM mart_hourly_profile "
        "ORDER BY city_name, hour_of_day",
    )
    health = _json_rows(con, "SELECT * FROM mart_pipeline_health")[0]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "latest": latest,
        "daily": daily,
        "profile": profile,
        "health": health,
        "checks": checks,
    }

    (EXPORT_DIR / "dashboard.json").write_text(
        json.dumps(payload, separators=(",", ":")), encoding="utf-8"
    )

    template = (DOCS_DIR / "_template.html").read_text(encoding="utf-8")
    html = template.replace("/*__DATA__*/null", json.dumps(payload, separators=(",", ":")))
    html = html.replace("__ATTRIBUTION__", ATTRIBUTION)
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")

    log.info("dashboard written to %s", DOCS_DIR / "index.html")
