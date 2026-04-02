CREATE VIEW silver_weather AS
SELECT
    id,
    city,
    ROUND(temperature::numeric, 2) AS temperature,
    humidity,
    LOWER(TRIM(weather_desc))      AS weather_desc,
    wind_speed,
    recorded_at,
    DATE(recorded_at)              AS full_date,
    EXTRACT(YEAR  FROM recorded_at)::INT AS year,
    EXTRACT(MONTH FROM recorded_at)::INT AS month,
    EXTRACT(DAY   FROM recorded_at)::INT AS day,
    EXTRACT(HOUR  FROM recorded_at)::INT AS hour,
    TO_CHAR(recorded_at, 'Day')         AS day_of_week,
    CASE
        WHEN EXTRACT(HOUR FROM recorded_at) BETWEEN 5  AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM recorded_at) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM recorded_at) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS period,
    CASE
        WHEN EXTRACT(HOUR FROM recorded_at) IN (7,8,9,17,18,19) THEN TRUE
        ELSE FALSE
    END AS is_peak_hour,
    CASE
        WHEN LOWER(weather_desc) LIKE '%rain%'      THEN 'Rain'
        WHEN LOWER(weather_desc) LIKE '%cloud%'     THEN 'Cloudy'
        WHEN LOWER(weather_desc) LIKE '%clear%'     THEN 'Clear'
        WHEN LOWER(weather_desc) LIKE '%storm%'     THEN 'Storm'
        WHEN LOWER(weather_desc) LIKE '%thunder%'   THEN 'Storm'
        ELSE 'Other'
    END AS category,
    CASE
        WHEN LOWER(weather_desc) LIKE '%storm%'     THEN 'High'
        WHEN LOWER(weather_desc) LIKE '%thunder%'   THEN 'High'
        WHEN LOWER(weather_desc) LIKE '%heavy%'     THEN 'Medium'
        WHEN LOWER(weather_desc) LIKE '%rain%'      THEN 'Low'
        ELSE 'Normal'
    END AS severity_level
FROM raw_weather;

INSERT INTO dim_date (full_date, year, month, day, hour, day_of_week)
SELECT DISTINCT
    full_date,
    year,
    month,
    day,
    hour,
    TRIM(day_of_week)
FROM silver_weather
WHERE full_date NOT IN (SELECT full_date FROM dim_date);

INSERT INTO dim_location (city, country, latitude, longitude)
SELECT DISTINCT
    city,
    'Vietnam' AS country,
    10.8231   AS latitude,
    106.6297  AS longitude
FROM silver_weather
WHERE city NOT IN (SELECT city FROM dim_location);

INSERT INTO dim_condition (description, category, severity_level)
SELECT DISTINCT
    weather_desc,
    category,
    severity_level
FROM silver_weather
WHERE weather_desc NOT IN (SELECT description FROM dim_condition);

INSERT INTO dim_time_of_day (time_label, period, is_peak_hour)
SELECT DISTINCT
    CONCAT(hour, ':00') AS time_label,
    period,
    is_peak_hour
FROM silver_weather
WHERE CONCAT(hour, ':00') NOT IN (SELECT time_label FROM dim_time_of_day);

INSERT INTO fact_weather (date_id, location_id, condition_id, time_id, temperature, humidity, wind_speed)
SELECT
    d.date_id,
    l.location_id,
    c.condition_id,
    t.time_id,
    s.temperature,
    s.humidity,
    s.wind_speed
FROM silver_weather s
JOIN dim_date     d ON d.full_date = s.full_date AND d.hour = s.hour
JOIN dim_location l ON l.city = s.city
JOIN dim_condition c ON c.description = s.weather_desc
JOIN dim_time_of_day t ON t.time_label = CONCAT(s.hour, ':00')
WHERE s.id NOT IN (
    SELECT rw.id FROM raw_weather rw
    JOIN dim_date d2 ON d2.full_date = DATE(rw.recorded_at) AND d2.hour = EXTRACT(HOUR FROM rw.recorded_at)
    JOIN fact_weather f ON f.date_id = d2.date_id
);

SELECT 
    f.weather_id,
    d.full_date,
    d.hour,
    d.day_of_week,
    l.city,
    l.country,
    c.description,
    c.category,
    c.severity_level,
    t.period,
    t.is_peak_hour,
    f.temperature,
    f.humidity,
    f.wind_speed
FROM fact_weather f
JOIN dim_date d         ON f.date_id = d.date_id
JOIN dim_location l     ON f.location_id = l.location_id
JOIN dim_condition c    ON f.condition_id = c.condition_id
JOIN dim_time_of_day t  ON f.time_id = t.time_id;

INSERT INTO dim_date (full_date, year, month, day, hour, day_of_week)
SELECT DISTINCT 
    DATE(recorded_at),
    EXTRACT(YEAR FROM recorded_at)::INT,
    EXTRACT(MONTH FROM recorded_at)::INT,
    EXTRACT(DAY FROM recorded_at)::INT,
    EXTRACT(HOUR FROM recorded_at)::INT,
    TRIM(TO_CHAR(recorded_at, 'Day'))
FROM raw_weather
WHERE DATE(recorded_at) || ' ' || EXTRACT(HOUR FROM recorded_at)::INT 
NOT IN (
    SELECT full_date::TEXT || ' ' || hour::TEXT FROM dim_date
);

SELECT COUNT(*) FROM raw_weather;
SELECT COUNT(*) FROM fact_weather;

SELECT raw_id, COUNT(*) 
FROM fact_weather 
GROUP BY raw_id 
HAVING COUNT(*) > 1;

DELETE FROM fact_weather
WHERE weather_id NOT IN (
    SELECT MIN(weather_id)
    FROM fact_weather
    GROUP BY raw_id
);