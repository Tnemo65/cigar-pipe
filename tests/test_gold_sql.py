# tests/test_gold_sql.py
from datetime import date, datetime
from decimal import Decimal

from src.transform.run_gold_sql import run_gold_sql_file

DIM_ZONE_ROWS = [(4, "Manhattan", "Alphabet City", "Yellow Zone", False)]
DIM_ZONE_COLUMNS = ["location_id", "borough", "zone", "service_zone", "is_sentinel"]

DIM_RATE_ROWS = [(1, "Standard", False), (2, "JFK", True)]
DIM_RATE_COLUMNS = ["rate_code_id", "rate_code_name", "is_flat_fare"]

DIM_PAY_ROWS = [(1, "Credit card", True), (2, "Cash", False)]
DIM_PAY_COLUMNS = ["payment_type_id", "payment_type_name", "tip_is_recorded"]


def _silver_row(**overrides):
    row = dict(
        trip_id="abc123",
        vendor_id=1,
        pickup_at=datetime(2024, 1, 15, 8, 0, 0),
        dropoff_at=datetime(2024, 1, 15, 8, 10, 0),
        pickup_location_id=4,
        dropoff_location_id=4,
        passenger_count=1,
        trip_distance_mi=2.5,
        rate_code_id=1,
        payment_type_id=1,
        fare_amount=Decimal("12.00"),
        extra_amount=Decimal("0.50"),
        mta_tax_amount=Decimal("0.50"),
        tip_amount=Decimal("2.00"),
        tolls_amount=Decimal("0.00"),
        improvement_surcharge_amount=Decimal("0.30"),
        congestion_surcharge_amount=Decimal("2.50"),
        cbd_congestion_fee_amount=Decimal("0.00"),
        total_amount=Decimal("15.30"),
        pickup_month=date(2024, 1, 1),
        _source_file="x.parquet",
        _ingested_at=datetime(2024, 2, 1, 0, 0, 0),
    )
    row.update(overrides)
    return row


def test_revenue_by_zone_hour_aggregates_correctly(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_zone.createOrReplaceTempView("dim_zone_test")

    silver = spark.createDataFrame(
        [_silver_row(), _silver_row(trip_id="def456", fare_amount=Decimal("8.00"))]
    )
    silver.createOrReplaceTempView("trips_clean_test")

    sql = (
        open("sql/gold/revenue_by_zone_hour.sql")
        .read()
        .replace("taxi_lakehouse.silver.trips_clean", "trips_clean_test")
        .replace("taxi_lakehouse.reference.dim_zone", "dim_zone_test")
        .replace(":month", "'2024-01-01'")
        .replace("taxi_lakehouse.gold.revenue_by_zone_hour", "gold_revenue_test")
    )
    spark.sql("DROP TABLE IF EXISTS gold_revenue_test")
    spark.sql(
        "CREATE TABLE gold_revenue_test (pickup_date DATE, pickup_hour INT, "
        "pickup_location_id INT, pickup_borough STRING, pickup_zone STRING, "
        "trip_count BIGINT, total_revenue DECIMAL(12,2), avg_fare_amount DECIMAL(10,2), "
        "avg_trip_distance_mi DOUBLE, pickup_month DATE) USING DELTA "
        "PARTITIONED BY (pickup_month)"
    )
    for statement in sql.split(";"):
        if statement.strip():
            spark.sql(statement)

    result = spark.table("gold_revenue_test").collect()[0]
    assert result.trip_count == 2
    assert result.total_revenue == Decimal("30.60")
    assert result.pickup_borough == "Manhattan"

    spark.sql("DROP TABLE gold_revenue_test")


def test_fare_integrity_daily_splits_flat_fare_from_standard(spark):
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_rate.createOrReplaceTempView("dim_rate_code_test")

    silver = spark.createDataFrame(
        [
            _silver_row(rate_code_id=1, fare_amount=Decimal("10.00"), trip_distance_mi=5.0),
            _silver_row(trip_id="jfk1", rate_code_id=2, fare_amount=Decimal("70.00"), trip_distance_mi=17.0),
        ]
    )
    silver.createOrReplaceTempView("trips_clean_test_2")

    sql = (
        open("sql/gold/fare_integrity_daily.sql")
        .read()
        .replace("taxi_lakehouse.silver.trips_clean", "trips_clean_test_2")
        .replace("taxi_lakehouse.reference.dim_rate_code", "dim_rate_code_test")
        .replace(":month", "'2024-01-01'")
        .replace("taxi_lakehouse.gold.fare_integrity_daily", "gold_fare_test")
    )
    spark.sql("DROP TABLE IF EXISTS gold_fare_test")
    spark.sql(
        "CREATE TABLE gold_fare_test (pickup_date DATE, is_flat_fare BOOLEAN, "
        "trip_count BIGINT, avg_fare_per_mile DOUBLE, fare_per_mile_p95 DOUBLE, "
        "pickup_month DATE) USING DELTA PARTITIONED BY (pickup_month)"
    )
    for statement in sql.split(";"):
        if statement.strip():
            spark.sql(statement)

    rows = {r.is_flat_fare: r for r in spark.table("gold_fare_test").collect()}
    assert rows[False].trip_count == 1
    assert rows[False].avg_fare_per_mile == 2.0  # 10.00 / 5.0
    assert rows[True].avg_fare_per_mile is None  # flat fares excluded from per-mile calc

    spark.sql("DROP TABLE gold_fare_test")


def test_payment_mix_monthly_only_computes_tip_pct_for_card(spark):
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    dim_pay.createOrReplaceTempView("dim_payment_type_test")

    silver = spark.createDataFrame(
        [
            _silver_row(payment_type_id=1, fare_amount=Decimal("10.00"), tip_amount=Decimal("2.00")),
            _silver_row(trip_id="cash1", payment_type_id=2, fare_amount=Decimal("10.00"), tip_amount=Decimal("0.00")),
        ]
    )
    silver.createOrReplaceTempView("trips_clean_test_3")

    sql = (
        open("sql/gold/payment_mix_monthly.sql")
        .read()
        .replace("taxi_lakehouse.silver.trips_clean", "trips_clean_test_3")
        .replace("taxi_lakehouse.reference.dim_payment_type", "dim_payment_type_test")
        .replace(":month", "'2024-01-01'")
        .replace("taxi_lakehouse.gold.payment_mix_monthly", "gold_payment_test")
    )
    spark.sql("DROP TABLE IF EXISTS gold_payment_test")
    spark.sql(
        "CREATE TABLE gold_payment_test (payment_type_name STRING, trip_count BIGINT, "
        "pct_of_month_trips DOUBLE, avg_tip_pct DOUBLE, pickup_month DATE) "
        "USING DELTA PARTITIONED BY (pickup_month)"
    )
    for statement in sql.split(";"):
        if statement.strip():
            spark.sql(statement)

    rows = {r.payment_type_name: r for r in spark.table("gold_payment_test").collect()}
    assert rows["Credit card"].avg_tip_pct == 0.2  # 2.00 / 10.00
    assert rows["Cash"].avg_tip_pct is None  # cash tips structurally unrecorded

    spark.sql("DROP TABLE gold_payment_test")
