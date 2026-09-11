"""Auto Loader stream: GCS raw Parquet → Bronze Delta table.

Two public functions for testability:
  add_lineage_columns(df)   — pure transform, no I/O
  build_bronze_stream(spark, config) → DataStreamWriter (caller calls .start())
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.streaming import DataStreamWriter

from src.common.paths import (
    catalog_table,
    checkpoint_path,
    raw_yellow_path,
    schema_location_path,
)
from src.common.schemas import BRONZE_SCHEMA


def add_lineage_columns(df: DataFrame) -> DataFrame:
    """Append _source_file and _ingested_at to a DataFrame (pure, no I/O)."""
    return df.withColumn(
        "_source_file", F.input_file_name()
    ).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def build_bronze_stream(spark: SparkSession, config: dict) -> DataStreamWriter:
    """Configure the Auto Loader readStream → Bronze Delta writeStream.

    Uses file-notification mode (cloudFiles.useNotifications=true) so GCS
    Pub/Sub pushes new-file events instead of polling the bucket directory.
    Schema hints avoid full inference on every stream restart.
    """
    raw_path = raw_yellow_path(config)
    ckpt = checkpoint_path("bronze", config)
    schema_loc = schema_location_path("bronze", config)
    target_table = catalog_table("bronze", "yellow_trips_raw")

    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(BRONZE_SCHEMA)
        .load(raw_path)
    )

    with_lineage = add_lineage_columns(raw_df)

    return (
        with_lineage.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
