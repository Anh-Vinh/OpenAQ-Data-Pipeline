-- SCHEMA
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;

-- COUNTRY
CREATE TABLE IF NOT EXISTS gold.dim_country (
    country_id BIGINT PRIMARY KEY,
    country_name TEXT,
    country_code TEXT,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS staging.staging_country (
    country_id BIGINT,
    country_name TEXT,
    country_code TEXT,
    ingestion_timestamp_utc TIMESTAMP,
    execution_date DATE
);

-- LOCATION
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_id BIGINT PRIMARY KEY,
    country_id BIGINT REFERENCES gold.dim_country(country_id),
    location_name TEXT,
    locality TEXT,
    timezone TEXT,
    datetime_first_utc TIMESTAMPTZ,
    datetime_last_utc TIMESTAMPTZ,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS staging.staging_location (
    location_id BIGINT,
    country_id BIGINT,
    location_name TEXT,
    locality TEXT,
    timezone TEXT,
    datetime_first_utc TIMESTAMPTZ,
    datetime_last_utc TIMESTAMPTZ,
    ingestion_timestamp_utc TIMESTAMP,
    execution_date DATE
);

-- PARAMETER
CREATE TABLE IF NOT EXISTS gold.dim_parameter (
    param_id INT PRIMARY KEY,
    param_name TEXT,
    param_units TEXT,
    param_display_name TEXT,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS staging.staging_parameter (
    param_id INT,
    param_name TEXT,
    param_units TEXT,
    param_display_name TEXT,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_date DATE
);

-- SENSOR
CREATE TABLE IF NOT EXISTS gold.dim_sensor (
    sensor_id BIGINT PRIMARY KEY,
    location_id BIGINT REFERENCES gold.dim_location(location_id),
    sensor_name TEXT,
    param_id INT REFERENCES gold.dim_parameter(param_id),
    -- param_name TEXT,
    -- param_units TEXT,
    -- param_display_name TEXT,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_date DATE
);

CREATE TABLE IF NOT EXISTS staging.staging_sensor (
    sensor_id BIGINT,
    location_id BIGINT,
    sensor_name TEXT,
    param_id INT,
    -- param_name TEXT,
    -- param_units TEXT,
    -- param_display_name TEXT,
    ingestion_timestamp_utc TIMESTAMP,
    execution_date DATE
);



-- MEASUREMENT
CREATE TABLE IF NOT EXISTS gold.fact_measurement (
    sensor_id BIGINT REFERENCES gold.dim_sensor(sensor_id),
    value DOUBLE PRECISION,
    param_id INT REFERENCES gold.dim_parameter(param_id),
    -- param_name TEXT,
    -- param_units TEXT,
    -- param_display_name TEXT,
    avg INT,
    max INT,
    min INT,
    datetime_from_utc TIMESTAMPTZ,
    datetime_to_utc TIMESTAMPTZ,
    ingestion_timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    measure_date DATE REFERENCES gold.dim_date(date_id),
    file_path TEXT,
    execution_date DATE,
    PRIMARY KEY (sensor_id, datetime_from_utc)
);

CREATE TABLE IF NOT EXISTS staging.staging_measurement (
    sensor_id BIGINT,
    value DOUBLE PRECISION,
    param_id INT,
    -- param_name TEXT,
    -- param_units TEXT,
    -- param_display_name TEXT,
    avg INT,
    max INT,
    min INT,
    datetime_from_utc TIMESTAMPTZ,
    datetime_to_utc TIMESTAMPTZ,
    ingestion_timestamp_utc TIMESTAMP,
    measure_date DATE,
    file_path TEXT,
    execution_date DATE
);