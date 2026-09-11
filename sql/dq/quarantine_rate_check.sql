-- sql/dq/quarantine_rate_check.sql
-- Production form -- table names are the real catalog tables, :month/:threshold
-- are Lakeflow Jobs SQL task parameters. design.md §11, §12.5.
SELECT assert_true(
  quarantined / (quarantined + clean) <= :threshold,
  concat('quarantine rate ', round(quarantined / (quarantined + clean) * 100, 2),
         '% exceeds threshold ', round(:threshold * 100, 2), '% for month ', :month)
)
FROM (
  SELECT
    (SELECT count(*) FROM taxi_lakehouse.silver.trips_quarantine
     WHERE date_trunc('month', tpep_pickup_datetime) = :month)  AS quarantined,
    (SELECT count(*) FROM taxi_lakehouse.silver.trips_clean
     WHERE pickup_month = :month)                                AS clean
);
