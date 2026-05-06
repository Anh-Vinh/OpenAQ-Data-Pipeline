BEGIN;

INSERT INTO gold.dim_location
SELECT *
FROM staging.staging_location
ON CONFLICT(location_id)
DO UPDATE SET
    country_id = EXCLUDED.country_id,
    location_name = EXCLUDED.location_name,
    locality = EXCLUDED.locality,
    timezone = EXCLUDED.timezone,
    datetime_first_utc = EXCLUDED.datetime_first_utc,
    datetime_last_utc  = EXCLUDED.datetime_last_utc,
    ingestion_timestamp_utc = EXCLUDED.ingestion_timestamp_utc,
    execution_date = EXCLUDED.execution_date;

TRUNCATE TABLE staging.staging_location;

COMMIT;