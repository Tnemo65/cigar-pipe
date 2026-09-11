-- sql/gold/fare_integrity_daily.sql
INSERT OVERWRITE TABLE taxi_lakehouse.gold.fare_integrity_daily
PARTITION (pickup_month = :month)
SELECT
  date_trunc('day', t.pickup_at) AS pickup_date,
  r.is_flat_fare,
  count(*)                       AS trip_count,
  avg(CASE WHEN NOT r.is_flat_fare AND t.trip_distance_mi > 0
           THEN t.fare_amount / t.trip_distance_mi END)               AS avg_fare_per_mile,
  percentile_approx(
    CASE WHEN NOT r.is_flat_fare AND t.trip_distance_mi > 0
         THEN t.fare_amount / t.trip_distance_mi END, 0.95
  )                                                                     AS fare_per_mile_p95
FROM taxi_lakehouse.silver.trips_clean t
JOIN taxi_lakehouse.reference.dim_rate_code r
  ON t.rate_code_id = r.rate_code_id
WHERE t.pickup_month = :month
GROUP BY date_trunc('day', t.pickup_at), r.is_flat_fare;
