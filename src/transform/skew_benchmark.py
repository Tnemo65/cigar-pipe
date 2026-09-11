# src/transform/skew_benchmark.py
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import paths

SALT_BUCKETS = 8


def run_baseline(spark: SparkSession) -> float:
    df = spark.table(paths.catalog_table("silver", "trips_clean"))
    start = time.time()
    df.groupBy("pickup_location_id", F.hour("pickup_at")).count().collect()
    return time.time() - start


def run_salted(spark: SparkSession) -> float:
    """design.md §9.1 -- salt the hot key, pre-aggregate on the salted key,
    then combine. This is the only real fix for aggregation skew (AQE's
    skew-join rule does not apply to a GROUP BY -- see design.md §9.1)."""
    df = spark.table(paths.catalog_table("silver", "trips_clean")).withColumn(
        "_salt", (F.rand() * SALT_BUCKETS).cast("int")
    )
    start = time.time()
    partial = df.groupBy("pickup_location_id", F.hour("pickup_at").alias("pickup_hour"), "_salt").count()
    partial.groupBy("pickup_location_id", "pickup_hour").agg(F.sum("count").alias("count")).collect()
    return time.time() - start


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    baseline_seconds = run_baseline(spark)
    salted_seconds = run_salted(spark)
    print(f"baseline: {baseline_seconds:.2f}s, salted: {salted_seconds:.2f}s")
    print(
        "Record these numbers, the Spark UI's max/median task-time ratio for "
        "both stages, and the data tier used, in benchmarks/results.md."
    )
