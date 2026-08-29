-- ============================================================
-- MARTS
-- Pre-aggregated tables shaped for the dashboard. The point of a
-- mart is that the consumer never has to know the star schema.
--
-- Every AVG below sums DECIMAL rather than DOUBLE. Floating point
-- addition is not associative, so a DOUBLE average depends on the
-- order the rows were summed in -- and that order is not stable:
-- the warehouse is rebuilt on every run and the rows land in a
-- different physical order each time. The sums differed in the last
-- bit, ROUND turned that into a flipped final digit, and published
-- numbers drifted between builds of identical raw data. DECIMAL
-- addition is exact, so the result no longer depends on row order.
-- MAX, COUNT and SUM over integers are already order-independent.
-- ============================================================

CREATE OR REPLACE TABLE mart_daily_city AS
SELECT
    f.city_id,
    c.city_name,
    c.governorate,
    c.aqi_grid,
    f.date_key,
    COUNT(*)                                            AS hours_observed,
    ROUND(AVG(CAST(f.pm2_5          AS DECIMAL(18,4))), 2) AS avg_pm2_5,
    ROUND(MAX(f.pm2_5), 2)                              AS max_pm2_5,
    ROUND(AVG(CAST(f.pm10           AS DECIMAL(18,4))), 2) AS avg_pm10,
    ROUND(AVG(CAST(f.us_aqi         AS DECIMAL(18,4))), 1) AS avg_us_aqi,
    ROUND(MAX(f.us_aqi), 1)                             AS max_us_aqi,
    ROUND(AVG(CAST(f.temperature_c  AS DECIMAL(18,4))), 1) AS avg_temp_c,
    ROUND(AVG(CAST(f.wind_speed_kmh AS DECIMAL(18,4))), 1) AS avg_wind_kmh,
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
--
-- Grouped by local hour, not UTC. The whole point of this table is to line
-- pollution up against human activity — traffic, cooking, the evening
-- inversion — and in UTC every one of those lands 2 or 3 hours off, which
-- makes the chart look like Cairo's rush hour is at two in the afternoon.
-- Around a DST switch one local hour collects an extra sample and another
-- collects none; sample_size shows it.
CREATE OR REPLACE TABLE mart_hourly_profile AS
SELECT
    c.city_name,
    c.aqi_grid,
    f.hour_of_day_local,
    ROUND(AVG(CAST(f.pm2_5  AS DECIMAL(18,4))), 2) AS avg_pm2_5,
    ROUND(AVG(CAST(f.us_aqi AS DECIMAL(18,4))), 1) AS avg_us_aqi,
    COUNT(*)                                      AS sample_size
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
