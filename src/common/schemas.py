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
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
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

# Silver: rename raw TLC column names to snake_case analytic names
SILVER_RENAME: dict[str, str] = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type_id",
}

# Columns that must be cast from DOUBLE (bronze) to DECIMAL(10,2) (silver/gold)
MONEY_COLUMNS_SILVER: list[str] = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]

# 12 business-key columns whose SHA-256 hash becomes trip_id
BUSINESS_KEY_COLUMNS: list[str] = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "payment_type_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
]
