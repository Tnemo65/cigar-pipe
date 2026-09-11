"""Validity rules applied to bronze rows before Silver MERGE.

Three composable steps (pure functions, no side-effects):
  1. with_trip_id             — SHA-256 of 12 business-key columns (Task 12)
  2. flag_implausible_trips   — physics / business bounds (Task 10)
  3. flag_unresolved_references — FK checks against reference tables (Task 11)

Each function operates on raw Bronze column names. `reason_code` STRING column
is populated when invalid (NULL means the row is valid).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from src.common.schemas import BUSINESS_KEY_COLUMNS


def with_trip_id(df: DataFrame) -> DataFrame:
    """design.md §7.2, §11 -- SHA-256 of the widened composite business key.
    Concatenates all 12 key values as strings with '|' separator so that
    (NULL, 1) and (1, NULL) produce different hashes."""
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in BUSINESS_KEY_COLUMNS],
    )
    return df.withColumn("trip_id", F.sha2(concat_expr, 256))


def flag_implausible_trips(df: DataFrame) -> DataFrame:
    """design.md §8 rule 1: negative fare, non-positive duration, distance/fare
    mismatch. Adds `reason_code`; leaves it NULL when none of the three apply.
    Order matters -- negative fare is checked first, matching the spec's own
    ordering of the three conditions."""
    duration_seconds = F.col("tpep_dropoff_datetime").cast("long") - F.col(
        "tpep_pickup_datetime"
    ).cast("long")
    return df.withColumn(
        "reason_code",
        F.when(F.col("fare_amount") < 0, F.lit("NEGATIVE_FARE"))
        .when(duration_seconds <= 0, F.lit("NONPOSITIVE_DURATION"))
        .when(
            (F.col("trip_distance") <= 0) & (F.col("fare_amount") > 0),
            F.lit("DISTANCE_FARE_MISMATCH"),
        )
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
