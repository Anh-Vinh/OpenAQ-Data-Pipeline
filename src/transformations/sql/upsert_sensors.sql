BEGIN;

INSERT INTO gold.dim_sensor
SELECT *
FROM staging.staging_sensor
ON CONFLICT(sensor_id)
DO UPDATE SET
    location_id = EXCLUDED.location_id,
    sensor_name = EXCLUDED.sensor_name,
    param_id = EXCLUDED.param_id,
    -- param_name = EXCLUDED.param_name,
    -- param_units = EXCLUDED.param_units,
    -- param_display_name = EXCLUDED.param_display_name,
    ingestion_timestamp_utc = EXCLUDED.ingestion_timestamp_utc,
    execution_date = EXCLUDED.execution_date;

TRUNCATE TABLE staging.staging_sensor;

COMMIT;