# src/transform/run_gold_sql.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from src.common import paths


def gold_ddls(catalog: str = "taxi_lakehouse") -> list[str]:
    return [ddl.replace("taxi_lakehouse", catalog) for ddl in GOLD_DDLS]


def run_gold_sql_file(
    spark: SparkSession,
    sql_path: str,
    month: str,
    catalog: str = "taxi_lakehouse",
) -> None:
    """design.md §12.5 -- runs one Gold mart's parameterized SQL for one
    month. :month substitution is a plain string replace here (Databricks SQL
    tasks handle real parameter binding; this is the local/portable runner)."""
    p = Path(sql_path)
    if not p.is_absolute() and not p.exists():
        p = _root / sql_path
    sql_text = (
        p.read_text()
        .replace("taxi_lakehouse", catalog)
        .replace(":month", f"'{month}'")
    )
    for statement in sql_text.split(";"):
        statement = statement.strip()
        if statement:
            spark.sql(statement)


GOLD_DDLS = [
    """
    CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.revenue_by_zone_hour (
      pickup_date DATE,
      pickup_hour INT,
      pickup_location_id INT,
      pickup_borough STRING,
      pickup_zone STRING,
      trip_count BIGINT,
      total_revenue DECIMAL(12,2),
      avg_fare_amount DECIMAL(10,2),
      avg_trip_distance_mi DOUBLE,
      pickup_month DATE
    ) USING DELTA PARTITIONED BY (pickup_month)
    """,
    """
    CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.fare_integrity_daily (
      pickup_date DATE,
      is_flat_fare BOOLEAN,
      trip_count BIGINT,
      avg_fare_per_mile DOUBLE,
      fare_per_mile_p95 DOUBLE,
      pickup_month DATE
    ) USING DELTA PARTITIONED BY (pickup_month)
    """,
    """
    CREATE TABLE IF NOT EXISTS taxi_lakehouse.gold.payment_mix_monthly (
      payment_type_name STRING,
      trip_count BIGINT,
      pct_of_month_trips DOUBLE,
      avg_tip_pct DOUBLE,
      pickup_month DATE
    ) USING DELTA PARTITIONED BY (pickup_month)
    """,
]


if __name__ == "__main__":
    from datetime import datetime
    from src.common.run_log import log_run

    started_at = datetime.now()
    status = "SUCCESS"

    cfg = paths.load_config()
    spark = SparkSession.builder.getOrCreate()

    try:
        catalog = paths.catalog_name(cfg)
        for ddl in gold_ddls(catalog):
            spark.sql(ddl)

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
            for sql_file in (
                "sql/gold/revenue_by_zone_hour.sql",
                "sql/gold/fare_integrity_daily.sql",
                "sql/gold/payment_mix_monthly.sql",
            ):
                run_gold_sql_file(spark, sql_file, m, paths.catalog_name(cfg))
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
