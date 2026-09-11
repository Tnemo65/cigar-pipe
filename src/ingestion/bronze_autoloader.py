"""Auto Loader stream from GCS raw Parquet to Bronze Delta table.

Pure function add_lineage_columns is decoupled for unit testing.
"""
from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from src.common import paths
from src.common.schemas import BRONZE_SCHEMA


def add_lineage_columns(df: DataFrame) -> DataFrame:
    """_source_file comes from Auto Loader's _metadata column in production;
    kept as a separate, unit-testable step so this function works on any
    DataFrame, streaming or batch, cloud or local."""
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.lit(None).cast("string"))
    return df.withColumn("_ingested_at", F.current_timestamp())


def build_bronze_stream(spark: SparkSession, config: dict):
    """design.md §6.1, §12.4 -- file-notification Auto Loader, schemaLocation +
    schemaHints (cbd_congestion_fee) + rescuedDataColumn,
    append to Bronze with mergeSchema, trigger=AvailableNow."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", paths.schema_location_path("bronze", config))
        .schema(BRONZE_SCHEMA)
        .option("cloudFiles.schemaHints", "cbd_congestion_fee DOUBLE")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    )
    if config.get("databricks", {}).get("use_notifications", False):
        reader = reader.option("cloudFiles.useNotifications", "true")

    raw = reader.load(paths.raw_yellow_path(config)).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )
    enriched = add_lineage_columns(raw)

    return (
        enriched.writeStream.format("delta")
        .option("checkpointLocation", paths.checkpoint_path("bronze", config))
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(paths.catalog_table("bronze", "trips_raw"))
    )


if __name__ == "__main__":
    cfg = paths.load_config()
    spark = SparkSession.builder.getOrCreate()
    query = build_bronze_stream(spark, cfg)
    query.awaitTermination()
    print("Bronze stream finished.")
