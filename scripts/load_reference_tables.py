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


def run_seed_sql_file(spark: SparkSession, path: str, catalog: str = "taxi_lakehouse") -> None:
    with open(path) as f:
        for statement in f.read().replace("taxi_lakehouse", paths.identifier(catalog)).split(";"):
            statement = statement.strip()
            if statement:
                spark.sql(statement)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load reference tables into Unity Catalog")
    p.add_argument("--config", default="configs/config.yaml")
    p.add_argument("--zone-csv", default=None, dest="zone_csv")
    p.add_argument("--skip-zone", action="store_true", dest="skip_zone")
    return p.parse_args(argv)


def seed_references(spark, cfg, zone_csv):
    from src.common.reference import validate_dimension
    zone = load_dim_zone(spark, zone_csv).withColumn("_loaded_at", F.current_timestamp())
    validate_dimension(zone, "location_id", ("borough", "zone", "is_sentinel"))
    zone.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(paths.catalog_table("reference", "dim_zone", cfg))
    for table in ("dim_rate_code", "dim_payment_type"):
        run_seed_sql_file(spark, str(Path(__file__).resolve().parents[1] / f"sql/reference/{table}.sql"), paths.catalog_name(cfg))
        name = paths.catalog_table("reference", table, cfg)
        if "_loaded_at" not in spark.table(name).columns:
            spark.sql(f"ALTER TABLE {name} ADD COLUMNS (_loaded_at TIMESTAMP)")
        spark.sql(f"UPDATE {name} SET _loaded_at = current_timestamp()")


if __name__ == "__main__":
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    seed_references(spark, cfg, cfg.get("zone_csv") or f"{paths.raw_ref_path(cfg)}taxi_zone_lookup.csv")
