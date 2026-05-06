BEGIN;

INSERT INTO gold.dim_country
SELECT *
FROM staging.staging_country
ON CONFLICT(country_id)
DO UPDATE SET
    country_name = EXCLUDED.country_name,
    country_code = EXCLUDED.country_code,
    ingestion_timestamp_utc = EXCLUDED.ingestion_timestamp_utc,
    execution_date = EXCLUDED.execution_date;

TRUNCATE TABLE staging.staging_country;

COMMIT;