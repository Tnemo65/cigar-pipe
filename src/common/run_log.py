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
) -> None:
    table_name = table_name or paths.catalog_table("reference", "pipeline_run_log")
    row = spark.createDataFrame(
        [(task_name, rows_in, rows_out, rows_quarantined, status, started_at, ended_at)],
        ["task_name", "rows_in", "rows_out", "rows_quarantined", "status", "started_at", "ended_at"],
    )
    row.write.format("delta").mode("append").saveAsTable(table_name)
