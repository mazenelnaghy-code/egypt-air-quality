# Egypt Air Quality Warehouse

An automated data pipeline that collects hourly air quality and weather readings
for eight Egyptian cities, models them into a star schema, tests the result, and
republishes a dashboard — every six hours, without a server.

**Live dashboard:** https://mazenelnaghy-code.github.io/egypt-air-quality/

---

## What it does

```
Open-Meteo API
      │
      ▼
  EXTRACT ──────►  data/raw/YYYY-MM-DD.jsonl        append-only, source of truth
      │
      ▼
 TRANSFORM ─────►  DuckDB warehouse                 staging → dims → facts → marts
      │
      ▼
   TEST ────────►  12 data quality assertions       failures stop the run
      │
      ▼
  PUBLISH ──────►  docs/  (dashboard + CSV exports) served by GitHub Pages
```

Orchestration is a GitHub Actions cron job. Public repositories get unlimited
Actions minutes, so the whole thing runs indefinitely at no cost.

---

## Design decisions worth explaining

**Raw data is never modified.** Extract writes what the API returned and nothing
else. Every downstream table is derived. If a transform has a bug, fix it and
rebuild — no data is lost.

**The warehouse is rebuilt from scratch on every run.** At this volume that costs
about a second, and it buys idempotency: any run produces exactly the same
warehouse. A failed or half-finished run is never a problem, and backfilling is
just another run.

**Reproducible means byte-identical, and that takes work.** Two rebuilds of the
same raw data used to publish different files. Two separate causes: a table has
no inherent row order, so unordered `COPY` wrote the rows shuffled; and DOUBLE
addition is not associative, so an `AVG` summed in a different physical order
came out a fraction different and `ROUND` turned that into a flipped last digit.
Exports now carry an explicit `ORDER BY`, and every `AVG` sums `DECIMAL`, which
is exact and therefore order-independent. A test rebuilds twice and compares
hashes. The visible symptom was worse than the churn: the dashboard derived its
series colours from row order, so every city changed colour on every run.

**Duplicates are expected, not prevented.** The API returns overlapping windows
every run, so the same (city, hour) arrives many times, sometimes with corrected
values. Staging keeps the most recently ingested version via
`ROW_NUMBER() ... ORDER BY ingested_at DESC`.

**Star schema, not one wide table.** `fact_hourly_air_quality` holds the
measurements at one row per city per hour. `dim_city` and `dim_date` hold the
attributes you group by. This is what makes "weekday vs weekend by governorate"
a join rather than a rewrite.

**The warehouse is UTC, and says so once.** Extract requests `timezone=UTC`, so
every timestamp stored is naive UTC. The DuckDB session is pinned to UTC in
`transform.py` for the same reason: `NOW()::TIMESTAMP` otherwise resolves to
whatever the machine's clock says, and comparing that against UTC data shifts
every "has this hour happened yet?" test by the local offset. Built in Egypt
(UTC+3) that put three hours of forecast into `mart_latest_city` while a UTC CI
runner saw none of it — the same raw data producing different marts depending on
where you ran it, which is exactly the property this pipeline claims to have.

**Forecast hours exist and are excluded on purpose.** Extract asks for
`forecast_days=1`, so the fact table always contains hours that have not
happened. That is deliberate: it means a reading is already stored before it is
observed, and the next run overwrites it with the actual value. Marts that
report observations filter it out. `mart_daily_city` therefore shows today as a
partial day — `hours_observed` is the honest count, not always 24.

**Two cities can be one measurement.** Air quality comes from CAMS on a roughly
11 km grid. Cairo and Giza are 4 km apart, so they resolve to a single cell and
return identical pollutant values for every hour, while their weather differs
because that model is finer. Both are worth keeping; counting one measurement
twice in a cross-city average is not. `dim_city.aqi_grid` marks cities that
share a cell, aggregates group by it, and a quality check fails if two cities
report identical readings without declaring it.

**Tests fail the build.** A pipeline without tests doesn't break loudly, it
quietly produces wrong numbers. Twelve assertions run against the built
warehouse; an error-level failure stops the run before the dashboard is
published. The unit tests build a throwaway warehouse in a temp directory —
they must never write to `data/raw/`, which CI commits.

---

## Layout

```
pipeline/
  config.py      cities, variables, paths — the only place to edit
  extract.py     API calls, retries, raw JSONL output
  transform.py   runs the SQL models against DuckDB
  quality.py     data quality assertions
  publish.py     CSV exports + dashboard rendering
  run.py         CLI entry point
sql/
  10_staging.sql     type casting and de-duplication
  20_dimensions.sql  dim_city, dim_date
  30_facts.sql       fact_hourly_air_quality
  40_marts.sql       pre-aggregated tables for the dashboard
tests/             offline tests using synthetic data
data/raw/          append-only JSONL partitions
docs/              published dashboard and CSV exports
```

The numeric prefixes on the SQL files are the dependency order. There is no
DAG framework here because four models in a fixed order don't need one.

---

## Running it locally

```bash
git clone https://github.com/mazenelnaghy-code/egypt-air-quality.git
cd egypt-air-quality

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m pipeline.run all         # extract → transform → test → publish
```

Then open `docs/index.html` in a browser.

Individual stages:

```bash
python -m pipeline.run extract     # fetch only
python -m pipeline.run build       # rebuild from existing raw data, no network
python -m pipeline.run test        # transform + quality checks
python -m pytest tests/ -q         # unit tests, works offline
```

Querying the warehouse directly:

```bash
python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb')
print(con.sql('''
    SELECT city_name, ROUND(AVG(avg_pm2_5), 1) AS pm25
    FROM mart_daily_city GROUP BY 1 ORDER BY 2 DESC
'''))
"
```

---

## Using the data

Every mart is published as CSV at a stable URL. No key, no sign-up.

```python
import pandas as pd

url = ("https://raw.githubusercontent.com/mazenelnaghy-code/egypt-air-quality/"
       "main/docs/data/mart_daily_city.csv")

df = pd.read_csv(url, parse_dates=["date_key"])
print(df.groupby("city_name").avg_pm2_5.mean().sort_values(ascending=False))
```

| File | Grain |
|---|---|
| `mart_daily_city.csv` | one row per city per day |
| `mart_latest_city.csv` | one row per city, most recent observed hour |
| `mart_hourly_profile.csv` | one row per city per hour-of-day |
| `dashboard.json` | everything the dashboard renders |

---

## Adding a city

Add an entry to `CITIES` in `pipeline/config.py` and run the pipeline. The
dimension table, dashboard, and exports all pick it up automatically — no other
file needs editing.

If the new city sits within ~11 km of one already listed, the air quality API
will return the same grid cell for both. The `no undeclared duplicate grid
cells` check warns when that happens; give the pair a shared `aqi_grid` value so
cross-city averages count the cell once.

---

## Stack

Python · DuckDB · SQL · GitHub Actions · GitHub Pages · Chart.js

---

## Attribution

Weather and air quality data by [Open-Meteo.com](https://open-meteo.com),
licensed under CC BY 4.0.

Built by Mazen Elnaghy.
