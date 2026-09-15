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
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.partitionColumns", "")
        .option("pathGlobFilter", "*.parquet")
        .option("rescuedDataColumn", "_rescued_data")
    )
    if config.get("databricks", {}).get("use_notifications", False):
        reader = reader.option("cloudFiles.useNotifications", "true")

    raw = reader.load(paths.raw_yellow_path(config)).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )
    enriched = (
        add_lineage_columns(raw)
        .withColumn("source_snapshot_id", F.regexp_extract("_source_file", r"/snapshot=([a-f0-9]{64})/", 1))
        .withColumn("batch_id", F.col("source_snapshot_id"))
        .withColumn("source_month", F.to_date(F.regexp_extract("_source_file", r"/source_month=(\d{4}-\d{2}-01)/", 1)))
        .withColumn("pipeline_run_id", F.lit(config["pipeline_run_id"]))
        .withColumn(
            "_source_object_id",
            F.sha2(F.col("_metadata.file_path"), 256),
        )
        .withColumn(
            "_source_object_version",
            F.regexp_extract("_source_file", r"/snapshot=([a-f0-9]{64})/", 1),
        )
    )

    return (
        enriched.writeStream.format("delta")
        .option("checkpointLocation", paths.checkpoint_path("bronze", config))
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(paths.catalog_table("bronze", "trips_raw", config))
    )


def run(spark, config):
    from src.common.run_state import execute_task
    def action(payload):
        query = build_bronze_stream(spark, config)
        query.awaitTermination()
        counts = []
        table = spark.table(paths.catalog_table("bronze", "trips_raw", config))
        for snapshot in payload["snapshots"]:
            count = table.filter(F.col("_source_file") == snapshot["uri"]).count()
            expected_rows = snapshot.get("rows_received")
            if expected_rows is not None and count != expected_rows:
                raise RuntimeError(f"Bronze source completeness mismatch: {snapshot['source_month']}")

            counts.append(dict(source_snapshot_id=snapshot["snapshot_id"], rows=count))
        return {**payload, "bronze_metrics": counts}
    return execute_task(spark, config, "ingest_bronze", "reference_preflight", action)


if __name__ == "__main__":
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
