-- Gold mart: revenue by pickup zone and hour of day for a given month.
-- Partitioned by pickup_month (YYYY-MM string) for idempotent INSERT OVERWRITE.
--
-- :month is substituted by run_gold_sql.py before execution (e.g. '2024-01').

CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.revenue_by_zone_hour (
    pickup_month        STRING      NOT NULL,
    pu_location_id      BIGINT      NOT NULL,
    zone                STRING,
    borough             STRING,
    hour_of_day         INT         NOT NULL,
    trip_count          BIGINT      NOT NULL,
    total_fare_amount   DECIMAL(18,2),
    total_tip_amount    DECIMAL(18,2),
    total_revenue       DECIMAL(18,2),
    avg_trip_distance   DOUBLE
)
USING DELTA
PARTITIONED BY (pickup_month);

INSERT OVERWRITE taxi_lakehouse.gold.revenue_by_zone_hour
PARTITION (pickup_month = ':month')
SELECT
    ':month'                                        AS pickup_month,
    t.pu_location_id,
    z.zone,
    z.borough,
    HOUR(t.tpep_pickup_datetime)                    AS hour_of_day,
    COUNT(*)                                        AS trip_count,
    SUM(t.fare_amount)                              AS total_fare_amount,
    SUM(t.tip_amount)                               AS total_tip_amount,
    SUM(t.fare_amount + t.tip_amount + t.tolls_amount
        + t.extra + t.mta_tax + t.improvement_surcharge
        + COALESCE(t.congestion_surcharge, 0)
        + COALESCE(t.airport_fee, 0)
        + COALESCE(t.cbd_congestion_fee, 0))        AS total_revenue,
    AVG(t.trip_distance)                            AS avg_trip_distance
FROM taxi_lakehouse.silver.yellow_trips t
LEFT JOIN taxi_lakehouse.reference.dim_zone z
    ON t.pu_location_id = z.location_id
WHERE DATE_FORMAT(t.tpep_pickup_datetime, 'yyyy-MM') = ':month'
GROUP BY
    t.pu_location_id,
    z.zone,
    z.borough,
    HOUR(t.tpep_pickup_datetime);
