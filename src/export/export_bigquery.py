# src/export/export_bigquery.py
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, functions as F

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from src.common import paths

Writer = Callable[[DataFrame, dict], None]


def _partition_export_path(options: dict, config: dict) -> str:
    """Return the authoritative BigLake path for one Gold table partition."""
    table_name = options["table"].split(".")[-1]
    partition = options.get("datePartition")
    if not partition:
        raise ValueError("datePartition is required for a partition-scoped export")
    return f"gs://{config['gcp']['bucket']}/gold_export/{table_name}/pickup_month={partition}"


def _publication_marker_path(export_path: str, config: dict) -> str:
    """Keep the commit marker outside the Parquet wildcard prefix."""
    table_name = export_path.rstrip("/").split("/")[-2]
    partition = export_path.rstrip("/").split("/")[-1]
    bucket = config["gcp"]["bucket"]
    return f"gs://{bucket}/gold_publication/{table_name}/{partition}/_PUBLISHED.json"


def _default_writer(df: DataFrame, options: dict) -> None:
    """Write one Gold partition to GCS for a BigLake external table.

    BigLake is the serving contract for this project.  There is deliberately no
    native BigQuery load fallback here: a table-wide WRITE_TRUNCATE or a
    driver-side toPandas() conversion could destroy historical partitions and
    cannot scale with the Gold data volume.
    """
    cfg = paths.load_config()
    export_path = _partition_export_path(options, cfg)
    table_name = options["table"].split(".")[-1]
    print(f"Writing {table_name} partition to {export_path}...")
    df.write.format("parquet").mode("overwrite").save(export_path)
    row_count = df.count()
    marker = {
        "table": table_name,
        "partition": options["datePartition"],
        "row_count": row_count,
        "export_path": export_path,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    marker_path = _publication_marker_path(export_path, cfg)
    from pyspark.dbutils import DBUtils

    DBUtils(df.sparkSession).fs.put(
        marker_path,
        json.dumps(marker, sort_keys=True),
        overwrite=True,
    )
    print(f"Successfully exported and published {table_name} partition to {export_path}.")

    # Spark writes marker files alongside data files.  They are not part of the
    # external table schema, but cleanup failure must remain visible as a warning.
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(df.sparkSession)
        for file_info in dbutils.fs.ls(export_path):
            if file_info.name.startswith("_"):
                dbutils.fs.rm(file_info.path)
    except Exception as cleanup_error:
        print(f"Warning: could not remove export marker files at {export_path}: {cleanup_error}")


def export_month(
    spark: SparkSession,
    gold_table: str,
    bq_dataset: str,
    month: str,
    writer: Writer = _default_writer,
) -> None:
    """Export only the Gold rows belonging to one touched month.

    The writer overwrites the corresponding GCS Hive partition, which is the
    BigLake serving boundary; it never truncates the complete serving table.
    """
    df = spark.table(gold_table).filter(F.col("pickup_month") == month)
    month_compact = month.replace("-", "")[:8] if len(month.replace("-", "")) >= 8 else month.replace("-", "") + "01"
    options = {
        "table": f"{bq_dataset}.{gold_table.split('.')[-1]}",
        "datePartition": month_compact,
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

            clean_tbl = paths.catalog_table("silver", "trips_clean", cfg)

        if spark.catalog.tableExists(clean_tbl):
            month_counts = {
                str(r.pickup_month)[:10]: r["count"]
                for r in spark.table(clean_tbl).groupBy("pickup_month").count().collect()
                if r.pickup_month
            }
            if not months:
                if cfg.get("ingestion", {}).get("allow_full_history_fallback", False):
                    months = [m for m, cnt in month_counts.items() if cnt >= 100]
                else:
                    months = []
            else:
                months = [m for m in months if month_counts.get(m, 0) >= 100]

        for m in months:
            for mart in ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly"):
                export_month(
                    spark,
                    paths.catalog_table("gold", mart, cfg),
                    bq_dataset,
                    m,
                )
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
