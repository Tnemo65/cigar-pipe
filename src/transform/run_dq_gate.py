# src/transform/run_dq_gate.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import paths


def compute_quarantine_rate(
    spark: SparkSession,
    month: str,
    clean_table: str = None,
    quarantine_table: str = None,
) -> float:
    clean_table = clean_table or paths.catalog_table("silver", "trips_clean")
    quarantine_table = quarantine_table or paths.catalog_table("silver", "trips_quarantine")

    clean_count = spark.sql(
        f"SELECT count(*) AS c FROM {clean_table} WHERE pickup_month = '{month}'"
    ).collect()[0]["c"]
    quarantine_count = spark.sql(
        f"SELECT count(*) AS c FROM {quarantine_table} "
        f"WHERE date_trunc('month', tpep_pickup_datetime) = '{month}'"
    ).collect()[0]["c"]

    total = clean_count + quarantine_count
    if total == 0:
        return 0.0
    return quarantine_count / total


def check_quarantine_rate(
    spark: SparkSession,
    month: str,
    threshold: float,
    clean_table: str = None,
    quarantine_table: str = None,
) -> None:
    """design.md §11 fail-fast gate. Raises RuntimeError (fails the Lakeflow
    Jobs task) if this month's quarantine rate exceeds threshold. Called once
    per touched month -- a breach halts only that month's Gold refresh."""
    rate = compute_quarantine_rate(spark, month, clean_table, quarantine_table)
    if rate > threshold:
        raise RuntimeError(
            f"quarantine rate {rate:.4f} exceeds threshold {threshold:.4f} for month {month}"
        )


if __name__ == "__main__":
    from datetime import datetime
    from src.common.run_log import log_run

    started_at = datetime.now()
    status = "SUCCESS"

    cfg = paths.load_config()
    spark = SparkSession.builder.getOrCreate()

    try:
        try:
            import dbutils  # type: ignore

            months = dbutils.jobs.taskValues.get(taskKey="transform_silver", key="touched_months")
        except ImportError:
            months = []  # local run -- pass months explicitly

        for m in months:
            check_quarantine_rate(spark, m, cfg["thresholds"]["quarantine_rate_max"])
        print(f"dq_gate passed for months: {months}")
    except Exception:
        status = "FAILED"
        raise
    finally:
        try:
            log_run(
                spark,
                task_name="dq_gate",
                rows_in=0,
                rows_out=0,
                rows_quarantined=0,
                status=status,
                started_at=started_at,
                ended_at=datetime.now(),
            )
        except Exception as log_error:
            print(f"run_log write failed (non-fatal): {log_error}")
