-- sql/reference/dim_rate_code.sql
-- Seed data for rate codes per TLC data dictionary.
-- Loaded by scripts/load_reference_tables.py into taxi_lakehouse.reference.dim_rate_code

CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.dim_rate_code (
    rate_code_id INT NOT NULL,
    rate_code_desc STRING NOT NULL
) USING DELTA;

INSERT OVERWRITE taxi_lakehouse.reference.dim_rate_code VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Unknown');
