"""Pipeline run logging — appended in each task's __main__ try/finally block.

log_run never raises; logging failure must never mask task failure.
"""
from __future__ import annotations

import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from src.common.paths import catalog_table

_LOG_TABLE = catalog_table("reference", "pipeline_run_log")


def log_run(
    *,
    task_name: str,
    month: str,
    status: str,           # "SUCCESS" | "FAILURE"
    rows_processed: int = 0,
    quarantine_rate: float | None = None,
    error_message: str | None = None,
) -> None:
    """Append one row to pipeline_run_log. Swallows all exceptions silently."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        row = [(task_name, month, status, rows_processed, quarantine_rate, error_message, now)]
        df = spark.createDataFrame(
            row,
            schema=(
                "task_name STRING, month STRING, status STRING, "
                "rows_processed LONG, quarantine_rate DOUBLE, "
                "error_message STRING, logged_at TIMESTAMP"
            ),
        )
        df.write.format("delta").mode("append").saveAsTable(_LOG_TABLE)
    except Exception:
        pass  # intentional: log failure must not surface to caller
