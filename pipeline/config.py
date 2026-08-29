"""Central configuration for the pipeline.

Everything that might change lives here, so no other module contains
hard-coded city names, URLs, or file paths.
"""
from pathlib import Path

# ---------------------------------------------------------------- paths
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
EXPORT_DIR = ROOT / "docs" / "data"
SQL_DIR = ROOT / "sql"
DOCS_DIR = ROOT / "docs"

# ---------------------------------------------------------------- source
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

# How many days of history to pull on each run. The API returns overlapping
# windows, which is intentional: late-arriving corrections get picked up and
# the transform layer de-duplicates. This is what makes reruns safe.
PAST_DAYS = 2

AIR_VARS = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "us_aqi",
]

WEATHER_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
]

# ---------------------------------------------------------------- cities
# governorate is a real attribute we can group by later, which is what makes
# dim_city worth having as a dimension rather than a plain text column.
#
# `aqi_grid` groups cities that the air quality model cannot tell apart.
# Open-Meteo serves air quality from CAMS on a roughly 11 km grid, so Cairo
# and Giza — 4 km apart — resolve to one cell and return byte-identical
# pollutant values for every hour. Their weather differs, because the weather
# model is finer, so both cities are worth keeping; what is not legitimate is
# counting one measurement twice in a cross-city average. Defaults to city_id,
# meaning "this city is its own cell".
CITIES = [
    {"city_id": "cairo",      "name": "Cairo",      "governorate": "Cairo",       "lat": 30.0444, "lon": 31.2357, "population": 9_500_000, "aqi_grid": "cairo_giza"},
    {"city_id": "alexandria", "name": "Alexandria", "governorate": "Alexandria",  "lat": 31.2001, "lon": 29.9187, "population": 5_400_000},
    {"city_id": "giza",       "name": "Giza",       "governorate": "Giza",        "lat": 30.0131, "lon": 31.2089, "population": 4_100_000, "aqi_grid": "cairo_giza"},
    {"city_id": "port_said",  "name": "Port Said",  "governorate": "Port Said",   "lat": 31.2653, "lon": 32.3019, "population": 750_000},
    {"city_id": "suez",       "name": "Suez",       "governorate": "Suez",        "lat": 29.9668, "lon": 32.5498, "population": 750_000},
    {"city_id": "mansoura",   "name": "Mansoura",   "governorate": "Dakahlia",    "lat": 31.0409, "lon": 31.3785, "population": 550_000},
    {"city_id": "luxor",      "name": "Luxor",      "governorate": "Luxor",       "lat": 25.6872, "lon": 32.6396, "population": 500_000},
    {"city_id": "aswan",      "name": "Aswan",      "governorate": "Aswan",       "lat": 24.0889, "lon": 32.8998, "population": 375_000},
]

# ---------------------------------------------------------------- quality
# WHO 2021 guideline values, used to classify readings in the marts layer.
WHO_PM25_GUIDELINE = 15.0   # ug/m3, 24-hour mean
WHO_PM10_GUIDELINE = 45.0   # ug/m3, 24-hour mean

ATTRIBUTION = "Weather and air quality data by Open-Meteo.com, CC BY 4.0"
