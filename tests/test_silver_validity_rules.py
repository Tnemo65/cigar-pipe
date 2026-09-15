from datetime import datetime

from src.transform.validity_rules import (
    flag_implausible_trips,
    flag_unresolved_references,
    with_trip_id,
)

ROW_TEMPLATE = dict(
    VendorID=1,
    tpep_pickup_datetime=datetime(2024, 1, 1, 8, 0, 0),
    tpep_dropoff_datetime=datetime(2024, 1, 1, 8, 10, 0),
    trip_distance=2.5,
    fare_amount=12.0,
    total_amount=15.0,
)

DIM_ZONE_ROWS = [
    (4, "Manhattan", "Alphabet City", "Yellow Zone", False),
    (264, "Unknown", "Unknown", "N/A", True),
]
DIM_ZONE_COLUMNS = ["location_id", "borough", "zone", "service_zone", "is_sentinel"]

DIM_RATE_CODE_ROWS = [(1, "Standard", False), (2, "JFK", True)]
DIM_RATE_CODE_COLUMNS = ["rate_code_id", "rate_code_name", "is_flat_fare"]

DIM_PAYMENT_TYPE_ROWS = [(1, "Credit card", True), (2, "Cash", False)]
DIM_PAYMENT_TYPE_COLUMNS = ["payment_type_id", "payment_type_name", "tip_is_recorded"]

FK_SCHEMA = "PULocationID INT, DOLocationID INT, RatecodeID INT, payment_type INT, reason_code STRING"


def _fk_row(**overrides):
    row = dict(
        PULocationID=4, DOLocationID=4, RatecodeID=1, payment_type=1, reason_code=None
    )
    row.update(overrides)
    return row


# --- Rule 1 tests ---

def test_negative_fare_is_flagged(spark):
    row = {**ROW_TEMPLATE, "fare_amount": -5.0}
    df = spark.createDataFrame([row])
    result = flag_implausible_trips(df).collect()[0]
    assert result.reason_code == "NEGATIVE_FARE"


def test_nonpositive_duration_is_flagged(spark):
    row = {**ROW_TEMPLATE, "tpep_dropoff_datetime": ROW_TEMPLATE["tpep_pickup_datetime"]}
    df = spark.createDataFrame([row])
    result = flag_implausible_trips(df).collect()[0]
    assert result.reason_code == "NONPOSITIVE_DURATION"


def test_distance_fare_mismatch_is_flagged(spark):
    row = {**ROW_TEMPLATE, "trip_distance": 0.0, "fare_amount": 12.0}
    df = spark.createDataFrame([row])
    result = flag_implausible_trips(df).collect()[0]
    assert result.reason_code == "DISTANCE_FARE_MISMATCH"


def test_plausible_trip_is_not_flagged(spark):
    df = spark.createDataFrame([ROW_TEMPLATE])
    result = flag_implausible_trips(df).collect()[0]
    assert result.reason_code is None


def test_negative_fare_wins_over_distance_mismatch_when_both_apply(spark):
    row = {**ROW_TEMPLATE, "fare_amount": -1.0, "trip_distance": 0.0}
    df = spark.createDataFrame([row])
    result = flag_implausible_trips(df).collect()[0]
    assert result.reason_code == "NEGATIVE_FARE"


# --- Rule 2 tests ---

def test_unknown_pickup_zone_is_flagged(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_CODE_ROWS, DIM_RATE_CODE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAYMENT_TYPE_ROWS, DIM_PAYMENT_TYPE_COLUMNS)
    df = spark.createDataFrame([_fk_row(PULocationID=99999)], schema=FK_SCHEMA)

    result = flag_unresolved_references(df, dim_zone, dim_rate, dim_pay).collect()[0]
    assert result.reason_code == "UNKNOWN_ZONE"


def test_sentinel_zone_is_flagged(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_CODE_ROWS, DIM_RATE_CODE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAYMENT_TYPE_ROWS, DIM_PAYMENT_TYPE_COLUMNS)
    df = spark.createDataFrame([_fk_row(PULocationID=264)], schema=FK_SCHEMA)

    result = flag_unresolved_references(df, dim_zone, dim_rate, dim_pay).collect()[0]
    assert result.reason_code == "SENTINEL_ZONE"


def test_unknown_rate_code_is_flagged(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_CODE_ROWS, DIM_RATE_CODE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAYMENT_TYPE_ROWS, DIM_PAYMENT_TYPE_COLUMNS)
    df = spark.createDataFrame([_fk_row(RatecodeID=99)], schema=FK_SCHEMA)

    result = flag_unresolved_references(df, dim_zone, dim_rate, dim_pay).collect()[0]
    assert result.reason_code == "UNKNOWN_RATE_CODE"


def test_unknown_payment_type_is_flagged(spark):
    dim_zone = spark.createDataFrame(DIM_ZONE_ROWS, DIM_ZONE_COLUMNS)
    dim_rate = spark.createDataFrame(DIM_RATE_CODE_ROWS, DIM_RATE_CODE_COLUMNS)
    dim_pay = spark.createDataFrame(DIM_PAYMENT_TYPE_ROWS, DIM_PAYMENT_TYPE_COLUMNS)
    df = spark.createDataFrame([_fk_row(payment_type=99)], schema=FK_SCHEMA)

    result = flag_unresolved_references(df, dim_zone, dim_rate, dim_pay).collect()[0]
    assert result.reason_code == "UNKNOWN_PAYMENT_TYPE"


# --- trip_id tests ---

def test_with_trip_id_is_deterministic(spark):
    row = {
        **ROW_TEMPLATE, "VendorID": 1, "PULocationID": 4, "DOLocationID": 7,
        "tip_amount": 2.0, "total_amount": 14.0, "RatecodeID": 1.0,
        "payment_type": 1, "passenger_count": 1.0
    }
    df1 = spark.createDataFrame([row])
    df2 = spark.createDataFrame([row])

    id1 = with_trip_id(df1).collect()[0].trip_id
    id2 = with_trip_id(df2).collect()[0].trip_id
    assert id1 == id2
    assert len(id1) == 64  # sha2/256 hex digest


def test_with_trip_id_differs_for_different_trips(spark):
    row_a = {
        **ROW_TEMPLATE, "VendorID": 1, "PULocationID": 4, "DOLocationID": 7,
        "tip_amount": 2.0, "total_amount": 14.0, "RatecodeID": 1.0,
        "payment_type": 1, "passenger_count": 1.0
    }
    row_b = {**row_a, "DOLocationID": 8}
    df = spark.createDataFrame([row_a, row_b])

    ids = [r.trip_id for r in with_trip_id(df).collect()]
    assert ids[0] != ids[1]
