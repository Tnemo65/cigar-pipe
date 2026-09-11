"""Skew benchmark: baseline GROUP BY vs salted GROUP BY on Silver trips.

run_baseline(spark, month)  — plain GROUP BY pu_location_id
run_salted(spark, month)    — adds a salt column to spread hot keys across SALT_BUCKETS partitions

Run as a Databricks notebook or spark-submit job, not in pytest (needs real data).
Results are printed; paste into benchmarks/results.md.
"""
from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from src.common.paths import catalog_table

SALT_BUCKETS = 8
_SILVER = catalog_table("silver", "yellow_trips")


def _load_month(spark: SparkSession, month: str) -> DataFrame:
    return (
        spark.table(_SILVER)
        .filter(F.date_format("tpep_pickup_datetime", "yyyy-MM") == month)
    )


def run_baseline(spark: SparkSession, month: str) -> dict:
    """Plain GROUP BY — exposes skew on hot pickup zones."""
    df = _load_month(spark, month)
    t0 = time.perf_counter()
    result = (
        df.groupBy("pu_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_fare"),
        )
    )
    row_count = result.count()
    elapsed = time.perf_counter() - t0
    return {"strategy": "baseline", "month": month, "rows": row_count, "seconds": round(elapsed, 2)}


def run_salted(spark: SparkSession, month: str) -> dict:
    """Salted GROUP BY — distributes hot keys across SALT_BUCKETS partitions then re-aggregates."""
    df = _load_month(spark, month)
    t0 = time.perf_counter()

    # Phase 1: partial aggregation with salt
    salted = df.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    partial = (
        salted.groupBy("pu_location_id", "salt")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_fare"),
        )
    )

    # Phase 2: final aggregation without salt
    result = (
        partial.groupBy("pu_location_id")
        .agg(
            F.sum("trip_count").alias("trip_count"),
            F.sum("total_fare").alias("total_fare"),
        )
    )
    row_count = result.count()
    elapsed = time.perf_counter() - t0
    return {"strategy": f"salted_{SALT_BUCKETS}", "month": month, "rows": row_count, "seconds": round(elapsed, 2)}


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--month", default="2024-01")
    args = p.parse_args()

    spark = SparkSession.builder.appName("skew-benchmark").getOrCreate()

    baseline = run_baseline(spark, args.month)
    salted = run_salted(spark, args.month)

    print("\n=== Skew Benchmark Results ===")
    for r in (baseline, salted):
        print(f"  {r['strategy']:20s}  rows={r['rows']:>8,}  time={r['seconds']:>6.2f}s")
    speedup = baseline["seconds"] / salted["seconds"] if salted["seconds"] > 0 else float("inf")
    print(f"\n  Speedup (salted vs baseline): {speedup:.2f}x")
    print("\nPaste these numbers into benchmarks/results.md")
