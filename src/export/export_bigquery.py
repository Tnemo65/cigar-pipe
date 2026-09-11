# src/export/export_bigquery.py
import sys
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import paths

Writer = Callable[[DataFrame, dict], None]


def _default_writer(df: DataFrame, options: dict) -> None:
    """design.md §12.2, §12.4 -- direct Storage Write API, no staging bucket,
    no BigQuery job permission needed. NOTE: verify `datePartition`'s exact
    option name/format against the pinned spark-bigquery-connector version's
    current docs before running against real BigQuery -- connector option
    names have shifted across versions."""
    writer = df.write.format("bigquery").option("writeMethod", "direct")
    for key, value in options.items():
        if key not in ("table",):
            writer = writer.option(key, value)
    writer.option("table", options["table"]).mode("overwrite").save()


def export_month(
    spark: SparkSession,
    gold_table: str,
    bq_dataset: str,
    month: str,
    writer: Writer = _default_writer,
) -> None:
    """design.md §8 rule 4, §11 layer 4 -- partition-scoped WRITE_TRUNCATE
    equivalent: filter to the touched month, overwrite only that partition
    decorator, not the whole table."""
    df = spark.table(gold_table).filter(F.col("pickup_month") == month)
    month_compact = month.replace("-", "")[:8] if len(month.replace("-", "")) >= 8 else month.replace("-", "") + "01"
    options = {
        "table": f"{bq_dataset}.{gold_table.split('.')[-1]}",
        "datePartition": month_compact,
        "writeMethod": "direct",
    }
    writer(df, options)


if __name__ == "__main__":
    from datetime import datetime
    from src.common.run_log import log_run

    started_at = datetime.now()
    status = "SUCCESS"

    cfg = paths.load_config()
    spark = SparkSession.builder.getOrCreate()
    bq_dataset = cfg["bigquery"]["dataset"]

    try:
        try:
            import dbutils  # type: ignore

            months = dbutils.jobs.taskValues.get(taskKey="transform_silver", key="touched_months")
        except ImportError:
            months = []

        for m in months:
            for mart in ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly"):
                export_month(spark, paths.catalog_table("gold", mart), bq_dataset, m)
        print(f"exported to BigQuery for months: {months}")
    except Exception:
        status = "FAILED"
        raise
    finally:
        try:
            log_run(
                spark,
                task_name="export_bigquery",
                rows_in=0,
                rows_out=0,
                rows_quarantined=0,
                status=status,
                started_at=started_at,
                ended_at=datetime.now(),
            )
        except Exception as log_error:
            print(f"run_log write failed (non-fatal): {log_error}")
