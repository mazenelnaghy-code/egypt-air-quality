-- ============================================================
-- STAGING
-- Read every raw JSONL partition, cast types, de-duplicate.
-- Rebuilt from scratch on every run, which is what makes the
-- pipeline idempotent: run it ten times, get the same warehouse.
-- ============================================================

CREATE OR REPLACE TABLE stg_readings AS
WITH raw AS (
    SELECT *
    FROM read_json_auto(
        getvariable('raw_glob'),
        union_by_name = true,
        format        = 'newline_delimited'
    )
),
typed AS (
    SELECT
        CAST(city_id              AS VARCHAR)   AS city_id,
        CAST(observed_at          AS TIMESTAMP) AS observed_at,
        CAST(ingested_at          AS TIMESTAMP) AS ingested_at,
        TRY_CAST(pm2_5            AS DOUBLE)    AS pm2_5,
        TRY_CAST(pm10             AS DOUBLE)    AS pm10,
        TRY_CAST(carbon_monoxide  AS DOUBLE)    AS carbon_monoxide,
        TRY_CAST(nitrogen_dioxide AS DOUBLE)    AS nitrogen_dioxide,
        TRY_CAST(sulphur_dioxide  AS DOUBLE)    AS sulphur_dioxide,
        TRY_CAST(ozone            AS DOUBLE)    AS ozone,
        TRY_CAST(us_aqi           AS DOUBLE)    AS us_aqi,
        TRY_CAST(temperature_2m       AS DOUBLE) AS temperature_c,
        TRY_CAST(relative_humidity_2m AS DOUBLE) AS humidity_pct,
        TRY_CAST(wind_speed_10m       AS DOUBLE) AS wind_speed_kmh
    FROM raw
    WHERE city_id IS NOT NULL
      AND observed_at IS NOT NULL
)
-- The API returns overlapping windows on every run, so the same
-- (city, hour) arrives many times. Keep the most recently ingested
-- version: later fetches contain corrected model output.
SELECT * EXCLUDE (rn)
FROM (
    SELECT
        typed.*,
        ROW_NUMBER() OVER (
            PARTITION BY city_id, observed_at
            ORDER BY ingested_at DESC
        ) AS rn
    FROM typed
)
WHERE rn = 1;
