BEGIN;

INSERT INTO gold.fact_measurement
SELECT *
FROM staging.staging_measurement
ON CONFLICT(sensor_id, datetime_from_utc)
DO UPDATE SET
    sensor_id = EXCLUDED.sensor_id,
    value = EXCLUDED.value,
    param_id = EXCLUDED.param_id,
    -- param_name = EXCLUDED.param_name,
    -- param_units = EXCLUDED.param_units,
    -- param_display_name = EXCLUDED.param_display_name,
    avg = EXCLUDED.avg,
    max = EXCLUDED.max,
    min = EXCLUDED.min,
    datetime_from_utc = EXCLUDED.datetime_from_utc,
    datetime_to_utc = EXCLUDED.datetime_to_utc,
    ingestion_timestamp_utc = EXCLUDED.ingestion_timestamp_utc,
    measure_date = EXCLUDED.measure_date,
    file_path = EXCLUDED.file_path,
    execution_date = EXCLUDED.execution_date;

TRUNCATE TABLE staging.staging_measurement;

COMMIT;