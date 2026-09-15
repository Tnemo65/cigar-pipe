"""Validity rules applied to bronze rows before Silver MERGE.

Three composable steps (pure functions, no side-effects):
  1. with_trip_id             — SHA-256 of 12 business-key columns (Task 12)
  2. flag_implausible_trips   — physics / business bounds (Task 10)
  3. flag_unresolved_references — FK checks against reference tables (Task 11)

Each function operates on raw Bronze column names. `reason_code` STRING column
is populated when invalid (NULL means the row is valid).
"""
from __future__ import annotations
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, functions as F

from src.common.schemas import BUSINESS_KEY_COLUMNS


def with_trip_id(df: DataFrame) -> DataFrame:
    """design.md §7.2, §11 -- SHA-256 of the widened composite business key.
    Concatenates all 12 key values as strings with '|' separator so that
    (NULL, 1) and (1, NULL) produce different hashes."""
    # Full payload identity within a monthly snapshot. Business key is only
    # diagnostic: TLC does not expose a unique trip ID suitable for upserts.
    payload_cols = sorted(c for c in df.columns if not c.startswith("_") and c not in
                          {"source_month", "source_snapshot_id", "pipeline_run_id", "batch_id"})
    concat_expr = F.to_json(F.struct(*[F.col(c) for c in payload_cols]), {"ignoreNullFields": "false"})
    with_id = df.withColumn("trip_id", F.sha2(concat_expr, 256))
    business_cols = [c for c in ("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID") if c in df.columns]
    with_id = with_id.withColumn("trip_business_key", F.sha2(F.to_json(F.struct(*business_cols)), 256))
    if "source_snapshot_id" in df.columns:
        with_id = with_id.withColumn("source_record_id", F.sha2(F.concat_ws("|", "source_snapshot_id", "trip_id"), 256))
    if "_source_object_id" in df.columns:
        with_id = with_id.withColumn(
            "_source_event_id",
            F.sha2(
                F.concat_ws("|", F.col("_source_object_id"), F.col("trip_id")),
                256,
            ),
        )
    return with_id


def flag_implausible_trips(df: DataFrame) -> DataFrame:
    """design.md §8 rule 1: negative fare, non-positive duration, distance/fare
    mismatch. Adds `reason_code`; leaves it NULL when none of the three apply.
    Order matters -- negative fare is checked first, matching the spec's own
    ordering of the three conditions."""
    required = ("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_distance", "total_amount")
    missing = reduce(or_, [F.col(c).isNull() if c in df.columns else F.lit(True) for c in required])
    numeric = [c for c in df.columns if c in {"trip_distance", "fare_amount", "total_amount", "tip_amount", "extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "Airport_fee", "airport_fee", "cbd_congestion_fee"}]
    nonfinite = reduce(or_, [(F.isnan(c) | (F.abs(F.col(c)) == float("inf"))) for c in numeric], F.lit(False))
    money_overflow = reduce(or_, [F.abs(F.col(c)) >= 99999999.995 for c in numeric if c != "trip_distance"], F.lit(False))
    rescue = F.col("_rescued_data").isNotNull() if "_rescued_data" in df.columns else F.lit(False)
    total = F.col("total_amount") if "total_amount" in df.columns else F.lit(None).cast("double")
    negotiated = F.coalesce(F.col("RatecodeID") == 5, F.lit(False)) if "RatecodeID" in df.columns else F.lit(False)
    outside_month = F.trunc("tpep_pickup_datetime", "month") != F.col("source_month") if "source_month" in df.columns else F.lit(False)
    duration_seconds = F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    return df.withColumn(
        "reason_code",
        F.when(rescue, F.lit("SCHEMA_RESCUED_DATA"))
        .when(missing, F.lit("MISSING_REQUIRED_FIELD"))
        .when(nonfinite | money_overflow, F.lit("INVALID_NUMERIC_VALUE"))
        .when(F.col("fare_amount") < 0, F.lit("NEGATIVE_FARE"))
        .when(duration_seconds <= 0, F.lit("NONPOSITIVE_DURATION"))
        .when(outside_month, F.lit("OUTSIDE_SOURCE_MONTH"))
        .when(total <= 0, F.lit("NONPOSITIVE_TOTAL"))
        .when(F.col("trip_distance") < 0, F.lit("NEGATIVE_DISTANCE"))
        .when(
            (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0) & ~negotiated,
            F.lit("DISTANCE_FARE_MISMATCH"),
        )
        .when((F.col("trip_distance") == 0) & (F.col("fare_amount") == 0), F.lit("ZERO_DISTANCE_ZERO_FARE"))
        .otherwise(F.lit(None).cast("string")),
    )


def flag_unresolved_references(
    df: DataFrame,
    dim_zone: DataFrame,
    dim_rate_code: DataFrame,
    dim_payment_type: DataFrame,
) -> DataFrame:
    """design.md §8 rule 2: PULocationID/DOLocationID must resolve against
    dim_zone (and not be a sentinel row), RatecodeID against dim_rate_code,
    payment_type against dim_payment_type. Only fills `reason_code` where
    rule 1 left it NULL -- never overwrites an existing flag."""
    pu_zone = dim_zone.select(
        F.col("location_id").alias("_pu_location_id"),
        F.col("is_sentinel").alias("_pu_is_sentinel"),
    )
    do_zone = dim_zone.select(
        F.col("location_id").alias("_do_location_id"),
        F.col("is_sentinel").alias("_do_is_sentinel"),
    )
    rate = dim_rate_code.select(F.col("rate_code_id").alias("_rate_code_id"))
    payment = dim_payment_type.select(F.col("payment_type_id").alias("_payment_type_id"))

    joined = (
        df.join(pu_zone, df.PULocationID == F.col("_pu_location_id"), "left")
        .join(do_zone, df.DOLocationID == F.col("_do_location_id"), "left")
        .join(rate, df.RatecodeID == F.col("_rate_code_id"), "left")
        .join(payment, df.payment_type == F.col("_payment_type_id"), "left")
    )

    fk_reason = (
        F.when(
            F.col("_pu_location_id").isNull() | F.col("_do_location_id").isNull(),
            F.lit("UNKNOWN_ZONE"),
        )
        .when(
            F.col("_pu_is_sentinel") | F.col("_do_is_sentinel"), F.lit("SENTINEL_ZONE")
        )
        .when(F.col("_rate_code_id").isNull(), F.lit("UNKNOWN_RATE_CODE"))
        .when(F.col("_payment_type_id").isNull(), F.lit("UNKNOWN_PAYMENT_TYPE"))
    )

    return joined.withColumn(
        "reason_code", F.coalesce(F.col("reason_code"), fk_reason)
    ).drop(
        "_pu_location_id",
        "_pu_is_sentinel",
        "_do_location_id",
        "_do_is_sentinel",
        "_rate_code_id",
        "_payment_type_id",
    )
