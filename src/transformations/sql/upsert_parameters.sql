BEGIN;

INSERT INTO gold.dim_parameter
SELECT *
FROM staging.staging_parameter
ON CONFLICT(param_id)
DO UPDATE SET
    param_id = EXCLUDED.param_id,
    param_name = EXCLUDED.param_name,
    param_units = EXCLUDED.param_units,
    param_display_name = EXCLUDED.param_display_name,
    ingestion_timestamp_utc = EXCLUDED.ingestion_timestamp_utc,
    execution_date = EXCLUDED.execution_date;

TRUNCATE TABLE staging.staging_parameter;

COMMIT;