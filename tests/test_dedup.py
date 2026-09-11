"""Tests: Silver MERGE deduplication via write_batch.

Two core cases:
  1. Re-running the exact same batch produces no duplicate rows.
  2. A new row (different trip_id) is inserted on the second run.
"""
from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transform.silver_clean import write_batch


# ──────────────────────────────────────────────────────────────
# Stub writer that accumulates rows in a list (no Delta needed)
# ──────────────────────────────────────────────────────────────

class _InMemoryWriter:
    """Emulates MERGE: insert-if-not-exists on trip_id."""

    def __init__(self) -> None:
        self._rows: list[dict] = []

    def merge(self, valid_df: DataFrame, _table: str) -> None:
        existing_ids = {r["trip_id"] for r in self._rows}
        new_rows = [r.asDict() for r in valid_df.collect() if r["trip_id"] not in existing_ids]
        self._rows.extend(new_rows)

    def append_quarantine(self, quar_df: DataFrame, _table: str) -> None:
        pass  # not under test here

    @property
    def row_count(self) -> int:
        return len(self._rows)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

_SILVER_SCHEMA = StructType([
    StructField("trip_id", StringType(), False),
    StructField("vendor_id", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("rate_code_id", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pu_location_id", LongType(), True),
    StructField("do_location_id", LongType(), True),
    StructField("payment_type_id", LongType(), True),
    StructField("fare_amount", DecimalType(10, 2), True),
    StructField("extra", DecimalType(10, 2), True),
    StructField("mta_tax", DecimalType(10, 2), True),
    StructField("tip_amount", DecimalType(10, 2), True),
    StructField("tolls_amount", DecimalType(10, 2), True),
    StructField("improvement_surcharge", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(10, 2), True),
    StructField("congestion_surcharge", DecimalType(10, 2), True),
    StructField("airport_fee", DecimalType(10, 2), True),
    StructField("cbd_congestion_fee", DecimalType(10, 2), True),
])


def _make_df(spark: SparkSession, trip_ids: list[str]) -> DataFrame:
    from decimal import Decimal
    from datetime import datetime

    rows = [
        (
            tid,
            1,
            datetime(2024, 1, 15, 10, 0, 0),
            datetime(2024, 1, 15, 10, 30, 0),
            2,
            3.5,
            1.0,
            "N",
            132,
            161,
            1,
            Decimal("14.50"),
            Decimal("0.50"),
            Decimal("0.50"),
            Decimal("2.00"),
            Decimal("0.00"),
            Decimal("1.00"),
            Decimal("18.50"),
            Decimal("2.50"),
            Decimal("0.00"),
            Decimal("0.00"),
        )
        for tid in trip_ids
    ]
    return spark.createDataFrame(rows, schema=_SILVER_SCHEMA)


# ──────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────

def test_no_duplicate_on_rerun(spark: SparkSession) -> None:
    """Writing the same trip_id twice must not produce duplicate rows."""
    writer = _InMemoryWriter()
    valid_df = _make_df(spark, ["abc123"])
    blank = _make_df(spark, []).filter("trip_id = 'none'")  # empty quarantine

    write_batch(valid_df, blank, writer=writer)
    assert writer.row_count == 1

    write_batch(valid_df, blank, writer=writer)  # same batch again
    assert writer.row_count == 1, "Duplicate row inserted on rerun"


def test_new_row_inserted(spark: SparkSession) -> None:
    """A batch with a new trip_id must be inserted alongside the existing one."""
    writer = _InMemoryWriter()
    blank = _make_df(spark, []).filter("trip_id = 'none'")

    write_batch(_make_df(spark, ["trip-1"]), blank, writer=writer)
    assert writer.row_count == 1

    write_batch(_make_df(spark, ["trip-2"]), blank, writer=writer)
    assert writer.row_count == 2, "New unique trip not inserted"
