-- Gold mart: payment mix monthly — share of trips and avg tip per payment type.
-- Partitioned by pickup_month for idempotent INSERT OVERWRITE.
--
-- pct_of_month_trips: window function over the full month partition.
-- avg_tip_pct: NULL for cash (payment_type_id=2) — tips not captured on cash.
-- :month substituted by run_gold_sql.py.

CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.payment_mix_monthly (
    pickup_month        STRING          NOT NULL,
    payment_type_id     BIGINT          NOT NULL,
    payment_label       STRING,
    trip_count          BIGINT          NOT NULL,
    pct_of_month_trips  DOUBLE          NOT NULL,
    avg_fare_amount     DECIMAL(18,2),
    avg_tip_amount      DECIMAL(18,2),
    avg_tip_pct         DOUBLE
)
USING DELTA
PARTITIONED BY (pickup_month);

INSERT OVERWRITE taxi_lakehouse.gold.payment_mix_monthly
PARTITION (pickup_month = ':month')
WITH monthly_agg AS (
    SELECT
        t.payment_type_id,
        p.payment_label,
        COUNT(*)                        AS trip_count,
        AVG(t.fare_amount)              AS avg_fare_amount,
        AVG(t.tip_amount)               AS avg_tip_amount,
        -- Cash tips not captured; NULL so consumers don't treat 0 as real data
        CASE
            WHEN t.payment_type_id = 2 THEN NULL
            ELSE AVG(t.tip_amount / NULLIF(t.fare_amount, 0))
        END                             AS avg_tip_pct
    FROM taxi_lakehouse.silver.yellow_trips t
    LEFT JOIN taxi_lakehouse.reference.dim_payment_type p
        ON t.payment_type_id = p.payment_type_id
    WHERE DATE_FORMAT(t.tpep_pickup_datetime, 'yyyy-MM') = ':month'
    GROUP BY t.payment_type_id, p.payment_label
)
SELECT
    ':month'                                                    AS pickup_month,
    payment_type_id,
    payment_label,
    trip_count,
    trip_count / SUM(trip_count) OVER ()                        AS pct_of_month_trips,
    avg_fare_amount,
    avg_tip_amount,
    avg_tip_pct
FROM monthly_agg;
