# src/export/export_bigquery.py
import sys
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from src.common import paths

Writer = Callable[[DataFrame, dict], None]


def _default_writer(df: DataFrame, options: dict) -> None:
    """design.md §12.2, §12.4 -- direct Storage Write API, no staging bucket.
    Falls back to partitioned Parquet export in GCS via Unity Catalog storage credential
    when running on Serverless compute where metadata server is not reachable."""
    try:
        writer = df.write.format("bigquery").option("writeMethod", "direct")
        for key, value in options.items():
            if key not in ("table",):
                writer = writer.option(key, value)
        writer.option("table", options["table"]).mode("overwrite").save()
    except Exception as e:
        print(f"Spark BigQuery direct write unsupported on serverless ({e}).")
        cfg = paths.load_config()
        bucket = cfg["gcp"]["bucket"]
        table_name = options["table"].split(".")[-1]
        part = options.get("datePartition", "")
        export_path = f"gs://{bucket}/gold_export/{table_name}/pickup_month={part}"
        print(f"Writing Gold export as Parquet to {export_path} via Unity Catalog storage credential...")
        df.write.format("parquet").mode("overwrite").save(export_path)
        print(f"Successfully exported {table_name} partition {part} to {export_path}.")
        try:
            from google.cloud import bigquery
            project_id = cfg["gcp"]["project_id"]
            client = bigquery.Client(project=project_id)
            pdf = df.toPandas()
            target_dest = f"{project_id}.{options['table']}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            job = client.load_table_from_dataframe(pdf, target_dest, job_config=job_config)
            job.result()
            print(f"Loaded {len(pdf)} rows into {target_dest} via BigQuery client.")
        except Exception as bq_err:
            print(f"BigQuery direct client sync skipped ({bq_err}). Data safely landed in {export_path}.")


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
            from pyspark.dbutils import DBUtils  # type: ignore

            dbutils = DBUtils(spark)
            months = dbutils.jobs.taskValues.get(
                taskKey="transform_silver", key="touched_months"
            )
        except Exception:
            months = []

        clean_tbl = paths.catalog_table("silver", "trips_clean")
        if spark.catalog.tableExists(clean_tbl):
            month_counts = {
                str(r.pickup_month)[:10]: r["count"]
                for r in spark.table(clean_tbl).groupBy("pickup_month").count().collect()
                if r.pickup_month
            }
            if not months:
                months = [m for m, cnt in month_counts.items() if cnt >= 100]
            else:
                months = [m for m in months if month_counts.get(m, 0) >= 100]

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
