"""Validity rules applied to bronze rows before Silver MERGE.

Three composable steps (pure functions, no side-effects):
  1. flag_implausible_trips   — physics / business bounds (Task 10)
  2. flag_unresolved_references — FK checks against reference tables (Task 11)
  3. with_trip_id             — SHA-256 of 12 business-key columns (Task 12)

Each function returns a DataFrame with a `reason_code` STRING column added
(NULL means the row is valid). Callers combine the three steps sequentially.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from src.common.schemas import BUSINESS_KEY_COLUMNS

# Reason codes used consistently from Task 10 onward (Global Constraints note)
_RC_NEG_FARE = "NEGATIVE_FARE"
_RC_ZERO_DIST = "ZERO_DISTANCE_NONZERO_FARE"
_RC_DIST_FARE = "DISTANCE_FARE_MISMATCH"
_RC_RATE_CODE = "UNKNOWN_RATE_CODE"
_RC_PAYMENT = "UNKNOWN_PAYMENT_TYPE"
_RC_LOCATION = "UNKNOWN_LOCATION"


def flag_implausible_trips(df: DataFrame) -> DataFrame:
    """Add reason_code for rows that violate physical / business plausibility rules.

    Rules (design.md §8 rule 1):
      - fare_amount < 0                        → NEGATIVE_FARE
      - trip_distance == 0 and fare_amount > 0 → ZERO_DISTANCE_NONZERO_FARE
      - trip_distance > 100 and fare_amount < 5 → DISTANCE_FARE_MISMATCH
    Only the first matching rule per row is recorded.
    """
    return df.withColumn(
        "reason_code",
        F.when(F.col("fare_amount") < 0, _RC_NEG_FARE)
        .when(
            (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
            _RC_ZERO_DIST,
        )
        .when(
            (F.col("trip_distance") > 100) & (F.col("fare_amount") < 5),
            _RC_DIST_FARE,
        )
        .otherwise(None),
    )


def flag_unresolved_references(
    df: DataFrame,
    valid_rate_codes: set[int],
    valid_payment_types: set[int],
    valid_location_ids: set[int],
) -> DataFrame:
    """Add reason_code where FK columns don't resolve to reference tables.

    Rules (design.md §8 rule 2):
      - rate_code_id not in dim_rate_code → UNKNOWN_RATE_CODE
      - payment_type_id not in dim_payment_type → UNKNOWN_PAYMENT_TYPE
      - pu_location_id or do_location_id not in dim_zone → UNKNOWN_LOCATION

    Rows already flagged (reason_code IS NOT NULL) keep their existing code.
    """
    rate_list = list(valid_rate_codes)
    payment_list = list(valid_payment_types)
    location_list = list(valid_location_ids)

    return df.withColumn(
        "reason_code",
        F.when(
            F.col("reason_code").isNotNull(),
            F.col("reason_code"),
        )
        .when(~F.col("rate_code_id").cast("int").isin(rate_list), _RC_RATE_CODE)
        .when(~F.col("payment_type_id").cast("int").isin(payment_list), _RC_PAYMENT)
        .when(
            ~F.col("pu_location_id").cast("int").isin(location_list)
            | ~F.col("do_location_id").cast("int").isin(location_list),
            _RC_LOCATION,
        )
        .otherwise(None),
    )


def with_trip_id(df: DataFrame) -> DataFrame:
    """Compute trip_id as SHA-256 of the 12 business-key columns (design.md §7.2).

    Concatenates all key values as strings with '|' separator before hashing
    so that (NULL, 1) and (1, NULL) produce different hashes.
    """
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in BUSINESS_KEY_COLUMNS],
    )
    return df.withColumn("trip_id", F.sha2(concat_expr, 256))
