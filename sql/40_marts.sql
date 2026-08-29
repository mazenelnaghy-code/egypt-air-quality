-- ============================================================
-- MARTS
-- Pre-aggregated tables shaped for the dashboard. The point of a
-- mart is that the consumer never has to know the star schema.
-- ============================================================

CREATE OR REPLACE TABLE mart_daily_city AS
SELECT
    f.city_id,
    c.city_name,
    c.governorate,
    f.date_key,
    COUNT(*)                                            AS hours_observed,
    ROUND(AVG(f.pm2_5), 2)                              AS avg_pm2_5,
    ROUND(MAX(f.pm2_5), 2)                              AS max_pm2_5,
    ROUND(AVG(f.pm10), 2)                               AS avg_pm10,
    ROUND(AVG(f.us_aqi), 1)                             AS avg_us_aqi,
    ROUND(MAX(f.us_aqi), 1)                             AS max_us_aqi,
    ROUND(AVG(f.temperature_c), 1)                      AS avg_temp_c,
    ROUND(AVG(f.wind_speed_kmh), 1)                     AS avg_wind_kmh,
    SUM(CASE WHEN f.exceeds_who_pm25 THEN 1 ELSE 0 END) AS hours_above_who
FROM fact_hourly_air_quality f
JOIN dim_city c USING (city_id)
-- The fact table carries forecast hours too (extract asks for forecast_days=1).
-- Averaging them in would make today's number part prediction while the column
-- is called hours_observed, and would move yesterday's number as the forecast
-- was replaced by actuals. Today is simply a partial day until it is over.
WHERE f.observed_at <= NOW()::TIMESTAMP
GROUP BY ALL;

CREATE OR REPLACE TABLE mart_latest_city AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT
        f.city_id,
        c.city_name,
        c.governorate,
        c.aqi_grid,
        c.latitude,
        c.longitude,
        f.observed_at,
        f.pm2_5,
        f.pm10,
        f.us_aqi,
        f.aqi_category,
        f.temperature_c,
        f.humidity_pct,
        f.wind_speed_kmh,
        ROW_NUMBER() OVER (
            PARTITION BY f.city_id ORDER BY f.observed_at DESC
        ) AS rn
    FROM fact_hourly_air_quality f
    JOIN dim_city c USING (city_id)
    WHERE f.observed_at <= NOW()::TIMESTAMP        -- exclude forecast hours
)
WHERE rn = 1;

-- Does pollution follow a daily rhythm? This is the kind of question
-- a star schema makes cheap to answer.
CREATE OR REPLACE TABLE mart_hourly_profile AS
SELECT
    c.city_name,
    f.hour_of_day,
    ROUND(AVG(f.pm2_5), 2)  AS avg_pm2_5,
    ROUND(AVG(f.us_aqi), 1) AS avg_us_aqi,
    COUNT(*)                AS sample_size
FROM fact_hourly_air_quality f
JOIN dim_city c USING (city_id)
WHERE f.observed_at <= NOW()::TIMESTAMP
GROUP BY ALL;

CREATE OR REPLACE TABLE mart_pipeline_health AS
SELECT
    (SELECT COUNT(*) FROM fact_hourly_air_quality)                AS fact_rows,
    (SELECT COUNT(DISTINCT city_id) FROM fact_hourly_air_quality) AS cities,
    (SELECT MIN(observed_at) FROM fact_hourly_air_quality)        AS earliest_observation,
    (SELECT MAX(observed_at) FROM fact_hourly_air_quality)        AS latest_observation,
    (SELECT MAX(ingested_at) FROM fact_hourly_air_quality)        AS last_ingest_at,
    NOW()::TIMESTAMP                                                          AS built_at;
