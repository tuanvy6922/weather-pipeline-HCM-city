-- Dim date
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    hour INT,
    day_of_week VARCHAR(20)
);

-- Dim location
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

-- Dim condition
CREATE TABLE dim_condition (
    condition_id SERIAL PRIMARY KEY,
    description VARCHAR(200),
    category VARCHAR(50),
    severity_level VARCHAR(20)
);

-- Dim time of day
CREATE TABLE dim_time_of_day (
    time_id SERIAL PRIMARY KEY,
    time_label VARCHAR(20),
    period VARCHAR(20),
    is_peak_hour BOOLEAN
);

-- Fact table
CREATE TABLE fact_weather (
    weather_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    condition_id INT REFERENCES dim_condition(condition_id),
    time_id INT REFERENCES dim_time_of_day(time_id),
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT
);

ALTER TABLE fact_weather ADD COLUMN raw_id INT;
UPDATE fact_weather f
SET raw_id = rw.id
FROM raw_weather rw
JOIN dim_date d ON d.full_date = DATE(rw.recorded_at)
    AND d.hour = EXTRACT(HOUR FROM rw.recorded_at)
WHERE f.date_id = d.date_id;