-- sql/gold/revenue_by_zone_hour.sql
-- Parameterized by :month (DATE, first day of the pickup month). Static
-- partition INSERT OVERWRITE, never a bare MERGE -- design.md §9.3, §12.5.
INSERT OVERWRITE TABLE taxi_lakehouse.gold.revenue_by_zone_hour
PARTITION (pickup_month = :month)
SELECT
  date_trunc('day', t.pickup_at)  AS pickup_date,
  hour(t.pickup_at)               AS pickup_hour,
  t.pickup_location_id,
  z.borough                       AS pickup_borough,
  z.zone                          AS pickup_zone,
  count(*)                        AS trip_count,
  sum(t.total_amount)             AS total_revenue,
  avg(t.fare_amount)              AS avg_fare_amount,
  avg(t.trip_distance_mi)         AS avg_trip_distance_mi
FROM taxi_lakehouse.silver.trips_clean t
JOIN taxi_lakehouse.reference.dim_zone z
  ON t.pickup_location_id = z.location_id
WHERE t.pickup_month = :month
GROUP BY date_trunc('day', t.pickup_at), hour(t.pickup_at), t.pickup_location_id, z.borough, z.zone;
