from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# design.md §7.3 — exact TLC schema, types as landed. cbd_congestion_fee is
# 2025+ only and nullable before that. Lineage columns (_rescued_data,
# _source_file, _ingested_at) are added by the Auto Loader stream (Task 13),
# not part of this hint schema.
BRONZE_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("cbd_congestion_fee", DoubleType(), True),
    ]
)

# design.md §7.3 — Bronze -> Silver rename map (naming convention, §7.1).
# Keys are Bronze (TLC-native) column names; values are Silver snake_case.
SILVER_RENAME: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance_mi",
    "RatecodeID": "rate_code_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra_amount",
    "mta_tax": "mta_tax_amount",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge_amount",
    "congestion_surcharge": "congestion_surcharge_amount",
    "cbd_congestion_fee": "cbd_congestion_fee_amount",
    "total_amount": "total_amount",
}

# design.md §7.3 — these become DECIMAL(10,2) in Silver (never DOUBLE, see
# Global Constraints). Named as their post-rename (Silver) column names.
MONEY_COLUMNS_SILVER: list[str] = [
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "congestion_surcharge_amount",
    "cbd_congestion_fee_amount",
    "total_amount",
]

# design.md §11 — the widened composite business key trip_id hashes.
# Bronze-native column names, since trip_id is computed before renaming (Task 12).
BUSINESS_KEY_COLUMNS: list[str] = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "RatecodeID",
    "payment_type",
    "passenger_count",
]
