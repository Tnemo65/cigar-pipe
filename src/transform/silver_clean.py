"""Silver cleaning: rename columns, cast money, dedup via MERGE.

process_batch  — pure transform; returns (valid_df, quarantine_df, touched_months)
write_batch    — does the DeltaTable MERGE + quarantine append (injectable for tests)
"""
from __future__ import annotations

from datetime import date
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, IntegerType

from src.common.paths import catalog_table
from src.common.schemas import (
    BUSINESS_KEY_COLUMNS,
    MONEY_COLUMNS_SILVER,
    SILVER_RENAME,
)
from src.transform.validity_rules import (
    flag_implausible_trips,
    flag_unresolved_references,
    with_trip_id,
)

_SILVER_TABLE = lambda: catalog_table("silver", "yellow_trips")      # noqa: E731
_QUAR_TABLE   = lambda: catalog_table("silver", "yellow_trips_quarantine")  # noqa: E731

# ──────────────────────────────────────────────────────────────
# Injectable writer protocol (real impl uses DeltaTable; tests stub it)
# ──────────────────────────────────────────────────────────────

class SilverWriter(Protocol):
    def merge(self, valid_df: DataFrame, target_table: str) -> None: ...
    def append_quarantine(self, quar_df: DataFrame, target_table: str) -> None: ...


class _DefaultSilverWriter:
    """Production writer backed by DeltaTable MERGE."""

    def merge(self, valid_df: DataFrame, target_table: str) -> None:
        from delta.tables import DeltaTable  # type: ignore[import]
        spark = valid_df.sparkSession
        delta_t = DeltaTable.forName(spark, target_table)
        (
            delta_t.alias("t")
            .merge(
                valid_df.alias("s"),
                "t.trip_id = s.trip_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def append_quarantine(self, quar_df: DataFrame, target_table: str) -> None:
        quar_df.write.format("delta").mode("append").saveAsTable(target_table)


# ──────────────────────────────────────────────────────────────
# Pure transform
# ──────────────────────────────────────────────────────────────

def _resolve_reference_sets(spark: SparkSession) -> tuple[set, set, set]:
    rate_codes = {r.rate_code_id for r in spark.table(catalog_table("reference", "dim_rate_code")).select("rate_code_id").collect()}
    payment_types = {r.payment_type_id for r in spark.table(catalog_table("reference", "dim_payment_type")).select("payment_type_id").collect()}
    location_ids = {r.location_id for r in spark.table(catalog_table("reference", "dim_zone")).select("location_id").collect()}
    return rate_codes, payment_types, location_ids


def process_batch(
    df: DataFrame,
    *,
    valid_rate_codes: set,
    valid_payment_types: set,
    valid_location_ids: set,
) -> tuple[DataFrame, DataFrame, list[str]]:
    """Rename, cast, flag, dedup-key → (valid_df, quarantine_df, touched_months).

    touched_months: list of 'YYYY-MM' strings for the affected pickup months.
    """
    # 1. Rename camelCase → snake_case
    renamed = df
    for old, new in SILVER_RENAME.items():
        renamed = renamed.withColumnRenamed(old, new)

    # 2. Cast money columns to DECIMAL(10,2)
    for col in MONEY_COLUMNS_SILVER:
        renamed = renamed.withColumn(col, F.col(col).cast(DecimalType(10, 2)))

    renamed = renamed.withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))

    # 3. Quality flags
    flagged = flag_implausible_trips(renamed)
    flagged = flag_unresolved_references(
        flagged, valid_rate_codes, valid_payment_types, valid_location_ids
    )

    # 4. trip_id
    with_id = with_trip_id(flagged)

    # 5. Split valid / quarantine
    valid_df = with_id.filter(F.col("reason_code").isNull()).drop("reason_code")
    quar_df  = with_id.filter(F.col("reason_code").isNotNull())

    # 6. Derive pickup_month partition label from valid rows
    touched = (
        valid_df.select(F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("m"))
        .distinct()
        .rdd.map(lambda r: r.m)
        .collect()
    )

    return valid_df, quar_df, sorted(touched)


# ──────────────────────────────────────────────────────────────
# I/O layer
# ──────────────────────────────────────────────────────────────

def write_batch(
    valid_df: DataFrame,
    quar_df: DataFrame,
    writer: SilverWriter | None = None,
) -> None:
    """MERGE valid rows into Silver; append quarantine rows."""
    w = writer or _DefaultSilverWriter()
    if valid_df.rdd.isEmpty() is False:
        w.merge(valid_df, _SILVER_TABLE())
    if quar_df.rdd.isEmpty() is False:
        w.append_quarantine(quar_df, _QUAR_TABLE())
