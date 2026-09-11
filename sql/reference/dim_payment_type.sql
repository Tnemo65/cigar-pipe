-- sql/reference/dim_payment_type.sql
-- Seed data for payment types per TLC data dictionary.
-- Loaded by scripts/load_reference_tables.py into taxi_lakehouse.reference.dim_payment_type

CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.dim_payment_type (
    payment_type_id INT NOT NULL,
    payment_type_desc STRING NOT NULL
) USING DELTA;

INSERT OVERWRITE taxi_lakehouse.reference.dim_payment_type VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip');
