# tests/test_dedup.py
from datetime import datetime

from delta.tables import DeltaTable

from src.transform.silver_clean import process_batch, write_batch

DIM_ZONE_ROWS = [(4, "Manhattan", "Alphabet City", "Yellow Zone", False)]
DIM_ZONE_COLUMNS = ["location_id", "borough", "zone", "service_zone", "is_sentinel"]
DIM_RATE_ROWS = [(1, "Standard", False)]
DIM_RATE_COLUMNS = ["rate_code_id", "rate_code_name", "is_flat_fare"]
DIM_PAY_ROWS = [(1, "Credit card", True)]
DIM_PAY_COLUMNS = ["payment_type_id", "payment_type_name", "tip_is_recorded"]

ROW = dict(
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


def test_rerunning_the_same_batch_does_not_duplicate_rows(spark, tmp_delta_path):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    batch = spark.createDataFrame([ROW])

    silver_valid, quarantine, _ = process_batch(batch, dim_zone, dim_rate, dim_pay)
    silver_valid.write.format("delta").save(tmp_delta_path)
    loc = tmp_delta_path.replace("\\", "/")
    spark.sql(f"CREATE TABLE local_silver_test USING DELTA LOCATION '{loc}'")

    try:
        target = DeltaTable.forName(spark, "local_silver_test")
        # First "run": table already has the row from the initial write above.
        assert spark.table("local_silver_test").count() == 1

        # Second "run": the same batch arrives again, e.g. a Workflow retry.
        (
            target.alias("t")
            .merge(silver_valid.alias("s"), "t.trip_id = s.trip_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

        assert spark.table("local_silver_test").count() == 1, (
            "reprocessing the identical batch must not duplicate the row"
        )
    finally:
        spark.sql("DROP TABLE IF EXISTS local_silver_test")


def test_a_genuinely_new_row_in_a_later_run_is_still_inserted(spark, tmp_delta_path):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_ROWS, DIM_RATE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAY_ROWS, DIM_PAY_COLUMNS)
    batch1 = spark.createDataFrame([ROW])
    silver_valid_1, _, _ = process_batch(batch1, dim_zone, dim_rate, dim_pay)
    silver_valid_1.write.format("delta").save(tmp_delta_path)
    loc2 = tmp_delta_path.replace("\\", "/")
    spark.sql(f"CREATE TABLE local_silver_test_2 USING DELTA LOCATION '{loc2}'")

    try:
        row2 = {
            **ROW,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 9, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 9, 10, 0),
        }
        batch2 = spark.createDataFrame([row2])
        silver_valid_2, _, _ = process_batch(batch2, dim_zone, dim_rate, dim_pay)

        target = DeltaTable.forName(spark, "local_silver_test_2")
        (
            target.alias("t")
            .merge(silver_valid_2.alias("s"), "t.trip_id = s.trip_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

        assert spark.table("local_silver_test_2").count() == 2, (
            "a genuinely different trip arriving in a later run must still be inserted"
        )
    finally:
        spark.sql("DROP TABLE IF EXISTS local_silver_test_2")
