from pyspark.sql.types import StructType

from src.common.schemas import BRONZE_SCHEMA


def test_bronze_schema_is_struct_type():
    assert isinstance(BRONZE_SCHEMA, StructType)


def test_bronze_schema_has_all_columns_from_design_doc():
    expected = {
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
        "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    }
    actual = {c.lower() for c in BRONZE_SCHEMA.fieldNames()}
    assert expected.issubset(actual)


def test_money_columns_are_double_in_bronze():
    for col_name in ("fare_amount", "tip_amount", "total_amount"):
        field = BRONZE_SCHEMA[col_name]
        assert field.dataType.typeName() == "double"
