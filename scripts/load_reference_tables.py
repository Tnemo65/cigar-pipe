"""Load reference dimension tables into Unity Catalog.

Runs dim_rate_code.sql and dim_payment_type.sql via Spark SQL, then
loads dim_zone from the TLC taxi zone lookup CSV.

LocationIDs 264 and 265 are sentinel rows (Unknown/NV) — flagged with
is_sentinel=True so validity rules can distinguish them from real boroughs.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import paths

SENTINEL_LOCATION_IDS = (264, 265)


def load_dim_zone(spark: SparkSession, csv_path: str) -> DataFrame:
    raw = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    return raw.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
        F.col("LocationID").isin(*SENTINEL_LOCATION_IDS).alias("is_sentinel"),
    )


def run_seed_sql_file(spark: SparkSession, path: str) -> None:
    with open(path) as f:
        for statement in f.read().split(";"):
            statement = statement.strip()
            if statement:
                spark.sql(statement)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load reference tables into Unity Catalog")
    p.add_argument("--config", default="configs/config.yaml")
    p.add_argument("--zone-csv", default=None, dest="zone_csv")
    p.add_argument("--skip-zone", action="store_true", dest="skip_zone")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    cfg = paths.load_config(args.config)
    spark = SparkSession.builder.getOrCreate()

    if not args.skip_zone:
        zone_csv = args.zone_csv or f"{paths.raw_ref_path(cfg)}taxi_zone_lookup.csv"
        dim_zone = load_dim_zone(spark, zone_csv)
        dim_zone.write.format("delta").mode("overwrite").saveAsTable(
            paths.catalog_table("reference", "dim_zone")
        )
        print("dim_zone loaded.")

    run_seed_sql_file(spark, "sql/reference/dim_rate_code.sql")
    run_seed_sql_file(spark, "sql/reference/dim_payment_type.sql")
    print("reference.* loaded.")
