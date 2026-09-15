-- sql/reference/dim_rate_code.sql
-- Static seed, run once by scripts/load_reference_tables.py.
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.dim_rate_code (
  rate_code_id   INT     NOT NULL COMMENT 'PK -- TLC RatecodeID',
  rate_code_name STRING  NOT NULL,
  is_flat_fare   BOOLEAN NOT NULL COMMENT 'true for JFK/Newark -- design.md §8 rule 3'
) USING DELTA;

TRUNCATE TABLE taxi_lakehouse.reference.dim_rate_code;

INSERT INTO taxi_lakehouse.reference.dim_rate_code (rate_code_id, rate_code_name, is_flat_fare) VALUES
  (1, 'Standard',              false),
  (2, 'JFK',                   true),
  (3, 'Newark',                true),
  (4, 'Nassau or Westchester', false),
  (5, 'Negotiated fare',       false),
  (6, 'Group ride',            false);
