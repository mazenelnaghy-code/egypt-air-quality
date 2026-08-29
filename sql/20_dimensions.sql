-- ============================================================
-- DIMENSIONS
-- Descriptive attributes you slice by. Small, wide, and stable.
-- ============================================================

CREATE OR REPLACE TABLE dim_city AS
SELECT
    CAST(city_id     AS VARCHAR) AS city_id,
    CAST(name        AS VARCHAR) AS city_name,
    CAST(governorate AS VARCHAR) AS governorate,
    CAST(lat         AS DOUBLE)  AS latitude,
    CAST(lon         AS DOUBLE)  AS longitude,
    CAST(population  AS BIGINT)  AS population,
    -- Cities sharing an aqi_grid share one air quality measurement. Any
    -- average across cities must group by this, not by city_id, or the
    -- shared cell is counted once per city that falls inside it.
    CAST(aqi_grid    AS VARCHAR) AS aqi_grid,
    CASE
        WHEN CAST(population AS BIGINT) >= 4000000 THEN 'mega'
        WHEN CAST(population AS BIGINT) >= 1000000 THEN 'large'
        ELSE 'mid'
    END AS size_band
FROM read_json_auto(getvariable('cities_json'));

-- A date dimension lets you answer "weekends vs weekdays" or
-- "which month is worst" with a join, instead of scattering date
-- arithmetic through every query.
CREATE OR REPLACE TABLE dim_date AS
SELECT
    d                             AS date_key,
    EXTRACT(year  FROM d)         AS year,
    EXTRACT(month FROM d)         AS month,
    EXTRACT(day   FROM d)         AS day,
    STRFTIME(d, '%Y-%m')          AS year_month,
    STRFTIME(d, '%A')             AS day_name,
    EXTRACT(dow FROM d) IN (5, 6) AS is_weekend   -- Fri/Sat in Egypt
FROM (
    SELECT UNNEST(RANGE(
        (SELECT MIN(observed_at)::DATE FROM stg_readings),
        (SELECT MAX(observed_at)::DATE + INTERVAL 1 DAY FROM stg_readings),
        INTERVAL 1 DAY
    ))::DATE AS d
);
