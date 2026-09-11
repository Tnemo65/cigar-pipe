-- Gold mart: fare integrity daily — flags flat-fare vs metered billing.
-- Partitioned by pickup_month for idempotent INSERT OVERWRITE.
--
-- is_flat_fare: rate_code_id IN (2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated)
-- :month substituted by run_gold_sql.py.

CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.fare_integrity_daily (
    pickup_month            STRING          NOT NULL,
    pickup_date             DATE            NOT NULL,
    is_flat_fare            BOOLEAN         NOT NULL,
    trip_count              BIGINT          NOT NULL,
    avg_fare_amount         DECIMAL(18,2),
    avg_tip_amount          DECIMAL(18,2),
    avg_trip_distance       DOUBLE,
    pct_with_tip            DOUBLE
)
USING DELTA
PARTITIONED BY (pickup_month);

INSERT OVERWRITE taxi_lakehouse.gold.fare_integrity_daily
PARTITION (pickup_month = ':month')
SELECT
    ':month'                                            AS pickup_month,
    DATE(tpep_pickup_datetime)                          AS pickup_date,
    rate_code_id IN (2, 3, 4, 5)                       AS is_flat_fare,
    COUNT(*)                                            AS trip_count,
    AVG(fare_amount)                                    AS avg_fare_amount,
    AVG(tip_amount)                                     AS avg_tip_amount,
    AVG(trip_distance)                                  AS avg_trip_distance,
    AVG(CASE WHEN tip_amount > 0 THEN 1.0 ELSE 0.0 END) AS pct_with_tip
FROM taxi_lakehouse.silver.yellow_trips
WHERE DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') = ':month'
GROUP BY
    DATE(tpep_pickup_datetime),
    rate_code_id IN (2, 3, 4, 5);
