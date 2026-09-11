from datetime import datetime

from src.transform.silver_clean import process_batch

DIM_ZONE_ROWS = [(4, "Manhattan", "Alphabet City", "Yellow Zone", False)]
DIM_ZONE_COLUMNS = ["location_id", "borough", "zone", "service_zone", "is_sentinel"]
DIM_RATE_ROWS = [(1, "Standard", False)]
DIM_RATE_COLUMNS = ["rate_code_id", "rate_code_name", "is_flat_fare"]
DIM_PAY_ROWS = [(1, "Credit card", True)]
DIM_PAY_COLUMNS = ["payment_type_id", "payment_type_name", "tip_is_recorded"]

VALID_ROW = dict(
    VendorID=1,
    tpep_pickup_datetime=datetime(2024, 1, 15, 8, 0, 0),
    tpep_dropoff_datetime=datetime(2024, 1, 15, 8, 10, 0),
    passenger_count=1.0,
    trip_distance=2.5,
    RatecodeID=1.0,
    PULocationID=4,
    DOLocationID=4,
    payment_type=1,
    fare_amount=12.0,
    extra=0.5,
    mta_tax=0.5,
    tip_amount=2.0,
    tolls_amount=0.0,
    improvement_surcharge=0.3,
    total_amount=15.3,
    congestion_surcharge=2.5,
    airport_fee=0.0,
    cbd_congestion_fee=0.0,
    _source_file="yellow_tripdata_2024-01.parquet",
    _ingested_at=datetime(2024, 2, 1, 0, 0, 0),
)


def test_process_batch_splits_valid_and_invalid(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)

    invalid_row = {**VALID_ROW, "fare_amount": -5.0}
    batch = spark.createDataFrame([VALID_ROW, invalid_row])

    silver_valid, quarantine, months = process_batch(batch, dim_zone, dim_rate, dim_pay)

    assert silver_valid.count() == 1
    assert quarantine.count() == 1
    assert quarantine.collect()[0].reason_code == "NEGATIVE_FARE"


def test_process_batch_renames_and_casts_money_to_decimal(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    batch = spark.createDataFrame([VALID_ROW])

    silver_valid, _, _ = process_batch(batch, dim_zone, dim_rate, dim_pay)
    row = silver_valid.collect()[0]

    assert row.pickup_location_id == 4
    assert row.rate_code_id == 1
    assert str(silver_valid.schema["fare_amount"].dataType) == "DecimalType(10,2)"
    assert row.pickup_month == datetime(2024, 1, 1).date()
    assert row.airport_fee_amount == 0.0


def test_process_batch_deduplicates_duplicate_source_events(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    duplicate = {**VALID_ROW, "_source_object_id": "object-1"}
    replay = {**VALID_ROW, "_source_object_id": "object-2"}
    batch = spark.createDataFrame([duplicate, replay])

    silver_valid, quarantine, _ = process_batch(batch, dim_zone, dim_rate, dim_pay)

    assert silver_valid.count() == 1
    assert quarantine.count() == 0


def test_process_batch_returns_distinct_touched_months(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    row_feb = {
        **VALID_ROW,
        "tpep_pickup_datetime": datetime(2024, 2, 1, 8, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 2, 1, 8, 10, 0),
    }
    batch = spark.createDataFrame([VALID_ROW, row_feb])

    _, _, months = process_batch(batch, dim_zone, dim_rate, dim_pay)

    assert sorted(months) == [datetime(2024, 1, 1).date(), datetime(2024, 2, 1).date()]
