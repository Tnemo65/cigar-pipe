# src/transform/run_gold_sql.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import paths


def run_gold_sql_file(spark: SparkSession, sql_path: str, month: str) -> None:
    """design.md §12.5 -- runs one Gold mart's parameterized SQL for one
    month. :month substitution is a plain string replace here (Databricks SQL
    tasks handle real parameter binding; this is the local/portable runner)."""
    sql_text = Path(sql_path).read_text().replace(":month", f"'{month}'")
    for statement in sql_text.split(";"):
        statement = statement.strip()
        if statement:
            spark.sql(statement)


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
            months = []

        for m in months:
            for sql_file in (
                "sql/gold/revenue_by_zone_hour.sql",
                "sql/gold/fare_integrity_daily.sql",
                "sql/gold/payment_mix_monthly.sql",
            ):
                run_gold_sql_file(spark, sql_file, m)
        print(f"gold marts refreshed for months: {months}")
    except Exception:
        status = "FAILED"
        raise
    finally:
        try:
            log_run(
                spark,
                task_name="aggregate_gold",
                rows_in=0,
                rows_out=0,
                rows_quarantined=0,
                status=status,
                started_at=started_at,
                ended_at=datetime.now(),
            )
        except Exception as log_error:
            print(f"run_log write failed (non-fatal): {log_error}")
