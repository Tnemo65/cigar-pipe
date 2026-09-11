-- sql/gold/payment_mix_monthly.sql
INSERT OVERWRITE TABLE taxi_lakehouse.gold.payment_mix_monthly
PARTITION (pickup_month = :month)
SELECT
  p.payment_type_name,
  count(*)                                              AS trip_count,
  count(*) / sum(count(*)) OVER ()                      AS pct_of_month_trips,
  avg(CASE WHEN p.tip_is_recorded
           THEN t.tip_amount / NULLIF(t.fare_amount, 0) END) AS avg_tip_pct
FROM taxi_lakehouse.silver.trips_clean t
JOIN taxi_lakehouse.reference.dim_payment_type p
  ON t.payment_type_id = p.payment_type_id
WHERE t.pickup_month = :month
GROUP BY p.payment_type_name;
