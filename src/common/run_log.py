# src/common/run_log.py
from datetime import datetime

from pyspark.sql import SparkSession

from src.common import paths


def log_run(
    spark: SparkSession,
    task_name: str,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    table_name: str = None,
    pipeline_run_id: str | None = None,
    batch_id: str | None = None,
    source_snapshot_id: str | None = None,
    rows_deduplicated: int = 0,
) -> None:
    table_name = table_name or paths.catalog_table("reference", "pipeline_run_log")
    row = spark.createDataFrame(
        [(
            pipeline_run_id,
            task_name,
            batch_id,
            source_snapshot_id,
            rows_in,
            rows_out,
            rows_deduplicated,
            rows_quarantined,
            status,
            started_at,
            ended_at,
        )],
        "pipeline_run_id STRING, task_name STRING, batch_id STRING, source_snapshot_id STRING, "
        "rows_in BIGINT, rows_out BIGINT, rows_deduplicated BIGINT, rows_quarantined BIGINT, "
        "status STRING, started_at TIMESTAMP, ended_at TIMESTAMP",
    )
    row.write.format("delta").mode("append").saveAsTable(table_name)
