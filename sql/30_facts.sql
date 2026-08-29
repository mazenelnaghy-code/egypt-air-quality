-- ============================================================
-- FACT TABLE
-- One row per city per hour: the grain of the warehouse.
-- Everything downstream aggregates from here.
-- ============================================================

CREATE OR REPLACE TABLE fact_hourly_air_quality AS
SELECT
    s.city_id,
    s.observed_at,
    s.observed_at::DATE              AS date_key,
    EXTRACT(hour FROM s.observed_at) AS hour_of_day,
    s.pm2_5,
    s.pm10,
    s.carbon_monoxide,
    s.nitrogen_dioxide,
    s.sulphur_dioxide,
    s.ozone,
    s.us_aqi,
    s.temperature_c,
    s.humidity_pct,
    s.wind_speed_kmh,
    -- Derived measures belong in the fact table when they are simple
    -- and depend only on this row.
    CASE
        WHEN s.us_aqi IS NULL THEN NULL
        WHEN s.us_aqi <=  50  THEN 'Good'
        WHEN s.us_aqi <= 100  THEN 'Moderate'
        WHEN s.us_aqi <= 150  THEN 'Unhealthy for sensitive groups'
        WHEN s.us_aqi <= 200  THEN 'Unhealthy'
        WHEN s.us_aqi <= 300  THEN 'Very unhealthy'
        ELSE 'Hazardous'
    END                                AS aqi_category,
    s.pm2_5 > getvariable('who_pm25')  AS exceeds_who_pm25,
    s.ingested_at
FROM stg_readings s
INNER JOIN dim_city c USING (city_id);   -- drops readings for unknown cities
