-- sql/reference/dim_payment_type.sql
-- Static seed, run once by scripts/load_reference_tables.py.
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.dim_payment_type (
  payment_type_id   INT     NOT NULL COMMENT 'PK -- TLC payment_type',
  payment_type_name STRING  NOT NULL,
  tip_is_recorded   BOOLEAN NOT NULL COMMENT 'true only for credit card -- design.md §8 rule 5'
) USING DELTA;

TRUNCATE TABLE taxi_lakehouse.reference.dim_payment_type;

INSERT INTO taxi_lakehouse.reference.dim_payment_type VALUES
  (1, 'Credit card', true),
  (2, 'Cash',        false),
  (3, 'No charge',   false),
  (4, 'Dispute',     false),
  (5, 'Unknown',     false),
  (6, 'Voided trip', false);
