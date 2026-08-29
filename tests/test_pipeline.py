"""Tests for the transform and quality layers.

These run without network access: they generate synthetic raw files,
build the warehouse from them, and assert on the result. That means CI
can catch a broken SQL model even when the API is down.

Everything here happens in a temporary directory. Tests must never touch
data/raw/ — that is the append-only source of truth, and CI commits it.
"""
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline import extract, quality, transform
from pipeline.config import CITIES


def make_fake_raw(
    raw_dir: Path, days: int = 4, seed: int = 7, forecast_hours: int = 12
) -> int:
    """Write plausible synthetic readings into `raw_dir`.

    `forecast_hours` of future readings are included on purpose: the real
    extract asks Open-Meteo for forecast_days=1, so the fact table always
    contains hours that have not happened yet. Marts that claim to report
    observations have to exclude them, and a fixture with no future rows
    cannot prove they do.
    """
    rng = random.Random(seed)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for old in raw_dir.glob("*.jsonl"):
        old.unlink()

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    ingested_at = now.isoformat(timespec="seconds")
    written = 0

    by_day: dict[str, list[str]] = {}
    for city in CITIES:
        # Bigger cities are dirtier; add a daily cycle and some noise.
        base = 12 + (city["population"] / 500_000)
        for hour in range(days * 24 + forecast_hours):
            ts = start + timedelta(hours=hour)
            cycle = math.sin((ts.hour - 6) / 24 * 2 * math.pi)
            pm25 = max(1.0, base + cycle * 8 + rng.gauss(0, 3))
            row = {
                "city_id": city["city_id"],
                "observed_at": ts.strftime("%Y-%m-%dT%H:%M"),
                "ingested_at": ingested_at,
                "pm2_5": round(pm25, 1),
                "pm10": round(pm25 * 1.8, 1),
                "carbon_monoxide": round(rng.uniform(150, 400), 1),
                "nitrogen_dioxide": round(rng.uniform(5, 40), 1),
                "sulphur_dioxide": round(rng.uniform(1, 15), 1),
                "ozone": round(rng.uniform(30, 90), 1),
                "us_aqi": round(min(300, pm25 * 3.2), 0),
                "temperature_2m": round(24 + cycle * 7 + rng.gauss(0, 1), 1),
                "relative_humidity_2m": round(rng.uniform(30, 80), 0),
                "wind_speed_10m": round(rng.uniform(3, 28), 1),
            }
            by_day.setdefault(ts.strftime("%Y-%m-%d"), []).append(
                json.dumps(row, separators=(",", ":"))
            )
            written += 1

    for day, lines in by_day.items():
        (raw_dir / f"{day}.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return written


@pytest.fixture(scope="module")
def sandbox(tmp_path_factory):
    """An isolated data/ directory: raw partitions plus a scratch warehouse."""
    root = tmp_path_factory.mktemp("warehouse")
    raw_dir = root / "raw"
    make_fake_raw(raw_dir)
    return raw_dir, root / "test.duckdb"


@pytest.fixture(scope="module")
def warehouse(sandbox):
    con = transform.run(*sandbox)
    yield con
    con.close()


def test_fact_table_has_rows(warehouse):
    assert warehouse.execute(
        "SELECT COUNT(*) FROM fact_hourly_air_quality"
    ).fetchone()[0] > 0


def test_grain_is_unique(warehouse):
    dupes = warehouse.execute(
        """SELECT COUNT(*) FROM (
               SELECT city_id, observed_at FROM fact_hourly_air_quality
               GROUP BY 1,2 HAVING COUNT(*) > 1)"""
    ).fetchone()[0]
    assert dupes == 0


def test_every_city_present(warehouse):
    n = warehouse.execute(
        "SELECT COUNT(DISTINCT city_id) FROM fact_hourly_air_quality"
    ).fetchone()[0]
    assert n == len(CITIES)


def test_dimension_join_is_complete(warehouse):
    orphans = warehouse.execute(
        """SELECT COUNT(*) FROM fact_hourly_air_quality f
           LEFT JOIN dim_city c USING (city_id) WHERE c.city_id IS NULL"""
    ).fetchone()[0]
    assert orphans == 0


def test_aqi_categories_are_valid(warehouse):
    bad = warehouse.execute(
        """SELECT COUNT(*) FROM fact_hourly_air_quality
           WHERE aqi_category IS NOT NULL AND aqi_category NOT IN
           ('Good','Moderate','Unhealthy for sensitive groups',
            'Unhealthy','Very unhealthy','Hazardous')"""
    ).fetchone()[0]
    assert bad == 0


def test_marts_are_populated(warehouse):
    for table in ["mart_daily_city", "mart_latest_city", "mart_hourly_profile"]:
        rows = warehouse.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert rows > 0, f"{table} is empty"


def test_rerun_is_idempotent(warehouse, sandbox):
    """Rebuilding from the same raw files must not change row counts."""
    before = warehouse.execute(
        "SELECT COUNT(*) FROM fact_hourly_air_quality"
    ).fetchone()[0]
    con = transform.run(*sandbox)
    after = con.execute("SELECT COUNT(*) FROM fact_hourly_air_quality").fetchone()[0]
    con.close()
    assert before == after


def test_fixture_actually_contains_forecast_hours(warehouse):
    """If this is zero, the two tests below prove nothing."""
    future = warehouse.execute(
        """SELECT COUNT(*) FROM fact_hourly_air_quality
           WHERE observed_at > (NOW() AT TIME ZONE 'UTC')"""
    ).fetchone()[0]
    assert future > 0


def test_latest_city_excludes_forecast_hours(warehouse):
    """The dashboard calls these current readings, so they must be observed.

    Regression test: the filter used to compare observed_at (UTC) against
    the machine's local wall clock, so every row was a forecast when built
    anywhere east of Greenwich.
    """
    forecasts = warehouse.execute(
        """SELECT COUNT(*) FROM mart_latest_city
           WHERE observed_at > (NOW() AT TIME ZONE 'UTC')"""
    ).fetchone()[0]
    assert forecasts == 0


def test_daily_averages_exclude_forecast_hours(warehouse):
    """mart_daily_city had no forecast filter at all, so today's mean was
    part prediction while the column was named hours_observed."""
    future_days = warehouse.execute(
        """SELECT COUNT(*) FROM mart_daily_city
           WHERE date_key > (NOW() AT TIME ZONE 'UTC')::DATE"""
    ).fetchone()[0]
    assert future_days == 0


def test_warehouse_session_is_utc(warehouse):
    """Everything downstream assumes naive timestamps are UTC."""
    offset = warehouse.execute(
        "SELECT NOW()::TIMESTAMP - (NOW() AT TIME ZONE 'UTC')"
    ).fetchone()[0]
    assert offset == timedelta(0), f"warehouse clock is {offset} off UTC"


def test_every_city_has_an_aqi_grid(warehouse):
    """Cross-city averages group by aqi_grid, so it can never be null."""
    missing = warehouse.execute(
        "SELECT COUNT(*) FROM dim_city WHERE aqi_grid IS NULL"
    ).fetchone()[0]
    assert missing == 0


def test_cities_sharing_a_grid_cell_are_declared(warehouse):
    """Any two cities returning identical pollutant readings for every hour
    are one measurement, and config must say so. This catches a new city
    being added on top of an existing grid cell."""
    undeclared = warehouse.execute(
        """SELECT a.city_id, b.city_id
           FROM fact_hourly_air_quality a
           JOIN fact_hourly_air_quality b USING (observed_at)
           JOIN dim_city ca ON ca.city_id = a.city_id
           JOIN dim_city cb ON cb.city_id = b.city_id
           WHERE a.city_id < b.city_id
             AND ca.aqi_grid <> cb.aqi_grid
           GROUP BY 1, 2
           HAVING COUNT(*) = SUM(CASE WHEN a.pm2_5 IS NOT DISTINCT FROM b.pm2_5
                                       AND a.us_aqi IS NOT DISTINCT FROM b.us_aqi
                                      THEN 1 ELSE 0 END)"""
    ).fetchall()
    assert not undeclared, f"identical readings, separate grids declared: {undeclared}"


def test_exports_are_byte_identical_across_rebuilds(sandbox, tmp_path, monkeypatch):
    """Same raw data must publish the same bytes.

    Tables have no inherent row order and DuckDB aggregates in parallel, so
    unordered exports came out shuffled on every build. CI then committed a
    diff of pure reordering every six hours, and the dashboard, which derived
    its series colours from row order, repainted every city a new colour.
    """
    import hashlib

    from pipeline import publish

    def build_once(out_dir):
        out_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(publish, "EXPORT_DIR", out_dir)
        con = transform.run(*sandbox)
        publish._export_tables(con)
        con.close()
        return {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(out_dir.glob("*.csv"))
        }

    first = build_once(tmp_path / "a")
    second = build_once(tmp_path / "b")

    # built_at is a genuine clock reading, so that one table may legitimately
    # differ; everything derived purely from the raw data must not.
    stable = {k: v for k, v in first.items() if k != "mart_pipeline_health.csv"}
    differing = [k for k, v in stable.items() if second[k] != v]
    assert not differing, f"non-deterministic exports: {differing}"


def test_local_hour_follows_egyptian_dst(warehouse):
    """The hourly profile is reported in local time, so the offset has to come
    from the IANA zone, not a hardcoded number: Egypt is UTC+3 in summer and
    UTC+2 in winter since DST returned in 2023."""
    summer, winter = warehouse.execute(
        """SELECT
             EXTRACT(hour FROM (TIMESTAMP '2026-08-29 14:00:00' AT TIME ZONE 'UTC')
                                AT TIME ZONE getvariable('local_tz')),
             EXTRACT(hour FROM (TIMESTAMP '2026-01-15 14:00:00' AT TIME ZONE 'UTC')
                                AT TIME ZONE getvariable('local_tz'))"""
    ).fetchone()
    assert (summer, winter) == (17, 16), f"got +{summer - 14}/+{winter - 14}"


def test_hourly_profile_covers_the_local_clock(warehouse):
    """Grouping by local hour must still span 00..23, not lose or invent one."""
    hours = warehouse.execute(
        "SELECT DISTINCT hour_of_day_local FROM mart_hourly_profile ORDER BY 1"
    ).fetchall()
    assert [h[0] for h in hours] == list(range(24))


def test_rows_land_in_the_partition_for_the_day_they_describe(tmp_path):
    """A partition's name must describe its contents, not when it was fetched.

    One fetch spans several observation dates -- past_days plus a forecast day
    -- so a single call has to split across files.
    """
    rows = [
        {"city_id": "cairo", "observed_at": "2026-08-27T23:00", "ingested_at": "x"},
        {"city_id": "cairo", "observed_at": "2026-08-28T00:00", "ingested_at": "x"},
        {"city_id": "giza",  "observed_at": "2026-08-28T05:00", "ingested_at": "x"},
        {"city_id": "cairo", "observed_at": "2026-08-29T12:00", "ingested_at": "x"},
    ]
    counts = extract._write_partitions(rows, tmp_path)

    assert counts == {"2026-08-27": 1, "2026-08-28": 2, "2026-08-29": 1}
    for path in tmp_path.glob("*.jsonl"):
        day = path.stem
        for line in path.read_text(encoding="utf-8").splitlines():
            assert json.loads(line)["observed_at"].startswith(day)


def test_partitions_are_append_only(tmp_path):
    """Re-fetching a day must add to its partition, never truncate it. The
    overlapping copies are what the staging de-duplication expects."""
    row = lambda ing: [
        {"city_id": "cairo", "observed_at": "2026-08-28T00:00", "ingested_at": ing}
    ]
    extract._write_partitions(row("first"), tmp_path)
    extract._write_partitions(row("second"), tmp_path)

    lines = (tmp_path / "2026-08-28.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(x)["ingested_at"] for x in lines] == ["first", "second"]


def test_compaction_leaves_the_write_window_alone(tmp_path):
    """Compressing a partition a future run still appends to would mean
    decompressing and rewriting it on every fetch."""
    today = datetime.now(timezone.utc).date()
    for offset in (0, 1, 2, 3, 10):
        day = today - timedelta(days=offset)
        (tmp_path / f"{day}.jsonl").write_text(
            json.dumps({"city_id": "cairo", "observed_at": f"{day}T00:00"}) + "\n",
            encoding="utf-8")

    extract.compact_partitions(tmp_path, keep_plain_days=3)

    plain = sorted(p.stem for p in tmp_path.glob("*.jsonl"))
    packed = sorted(p.name.removesuffix(".jsonl.gz") for p in tmp_path.glob("*.jsonl.gz"))
    assert plain == sorted(str(today - timedelta(days=o)) for o in (0, 1, 2))
    assert packed == sorted(str(today - timedelta(days=o)) for o in (3, 10))


def test_compaction_preserves_every_row(tmp_path):
    old = datetime.now(timezone.utc).date() - timedelta(days=30)
    rows = [{"city_id": "cairo", "observed_at": f"{old}T{h:02d}:00", "pm2_5": h}
            for h in range(24)]
    (tmp_path / f"{old}.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    extract.compact_partitions(tmp_path)

    import gzip
    with gzip.open(tmp_path / f"{old}.jsonl.gz", "rt", encoding="utf-8") as fh:
        assert [json.loads(x) for x in fh.read().splitlines()] == rows


def test_backfill_into_compressed_history_is_merged_not_split(tmp_path):
    """A backfill reaching into archived days writes a plain file next to the
    archive. The next compaction has to fold them together, or that date ends
    up half in one file and half in the other."""
    old = datetime.now(timezone.utc).date() - timedelta(days=30)
    row = lambda ing: json.dumps(
        {"city_id": "cairo", "observed_at": f"{old}T00:00", "ingested_at": ing})

    (tmp_path / f"{old}.jsonl").write_text(row("first") + "\n", encoding="utf-8")
    extract.compact_partitions(tmp_path)
    (tmp_path / f"{old}.jsonl").write_text(row("second") + "\n", encoding="utf-8")
    extract.compact_partitions(tmp_path)

    import gzip
    with gzip.open(tmp_path / f"{old}.jsonl.gz", "rt", encoding="utf-8") as fh:
        ingests = [json.loads(x)["ingested_at"] for x in fh.read().splitlines()]
    assert ingests == ["first", "second"]
    assert not list(tmp_path.glob("*.jsonl"))


def test_compressed_archives_are_byte_reproducible(tmp_path):
    """gzip embeds an mtime and filename by default, which would make the
    archive of a finished day churn in git on every rewrite."""
    old = datetime.now(timezone.utc).date() - timedelta(days=30)
    payload = json.dumps({"city_id": "cairo", "observed_at": f"{old}T00:00"}) + "\n"

    made = []
    for name in ("a", "b"):
        run_dir = tmp_path / name
        run_dir.mkdir()
        (run_dir / f"{old}.jsonl").write_text(payload, encoding="utf-8")
        extract.compact_partitions(run_dir)
        made.append((run_dir / f"{old}.jsonl.gz").read_bytes())

    assert made[0] == made[1]


def test_warehouse_is_identical_whether_or_not_raw_is_compressed(sandbox, tmp_path):
    """The whole point: compaction is a storage detail the models never see."""
    raw_dir, _ = sandbox

    mixed = tmp_path / "mixed"
    mixed.mkdir()
    for src in raw_dir.glob("*.jsonl"):
        (mixed / src.name).write_bytes(src.read_bytes())
    # force everything old enough to be archived, leaving a mixed directory
    extract.compact_partitions(mixed, keep_plain_days=1)
    assert list(mixed.glob("*.jsonl.gz")), "fixture produced nothing to compress"

    def fingerprint(directory, name):
        con = transform.run(directory, tmp_path / f"{name}.duckdb")
        out = con.execute(
            "SELECT * FROM mart_daily_city ORDER BY date_key, city_id"
        ).fetchall()
        con.close()
        return out

    assert fingerprint(mixed, "mixed") == fingerprint(raw_dir, "plain")


def test_tests_do_not_touch_real_raw_data(sandbox):
    """Guard the guard: the suite must build from its own directory.

    Without this, a test run wipes data/raw/ and CI commits synthetic
    readings over the real history.
    """
    from pipeline.config import RAW_DIR

    raw_dir, warehouse_path = sandbox
    assert RAW_DIR not in raw_dir.parents and raw_dir != RAW_DIR
    assert warehouse_path.parent != RAW_DIR.parent


def test_quality_checks_pass(warehouse):
    results = quality.run(warehouse)
    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    assert not errors, errors
