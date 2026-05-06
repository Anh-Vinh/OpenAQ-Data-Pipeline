CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

INSERT INTO gold.dim_date (date_id, day, month, year, day_of_week, is_weekend)
SELECT 
    d::date,
    EXTRACT(DAY FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(YEAR FROM d),
    EXTRACT(DOW FROM d),
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN true ELSE false END
FROM generate_series('2025-01-01', '2026-4-2', INTERVAL '1 day') d;