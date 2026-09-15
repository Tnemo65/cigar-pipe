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
    from src.common.runtime import month_start
    month_start(month)
    paths.identifier(catalog)
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


MARTS = ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly")


def run(spark, config):
    from pyspark.sql import functions as F
    from src.common.reference import load_references
    from src.common.run_state import execute_task
    def action(payload):
        load_references(spark, config)
        catalog = paths.catalog_name(config)
        for ddl in gold_ddls(catalog):
            spark.sql(ddl)
        counts = []
        for month in payload["months"]:
            clean = spark.table(paths.catalog_table("silver", "trips_clean", config)).filter(F.col("pickup_month") == month)
            expected = clean.agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")).first()
            for mart in MARTS:
                run_gold_sql_file(spark, f"sql/gold/{mart}.sql", month, catalog)
                gold = spark.table(paths.catalog_table("gold", mart, config)).filter(F.col("pickup_month") == month)
                actual = gold.agg(F.count("*").alias("rows"), F.sum("trip_count").alias("trips")).first()
                if (actual.trips or 0) != expected.trips:
                    raise RuntimeError(f"Gold trip reconciliation failed: {mart}/{month}")
                if mart == "revenue_by_zone_hour":
                    revenue = gold.agg(F.sum("total_revenue")).first()[0]
                    if (revenue or 0) != (expected.revenue or 0):
                        raise RuntimeError(f"Gold revenue reconciliation failed: {month}")
                snapshot_ids = [
                    snapshot["snapshot_id"]
                    for snapshot in payload.get("snapshots", [])
                    if snapshot["source_month"] == month
                ]
                counts.append(
                    dict(
                        mart=mart,
                        month=month,
                        rows=actual.rows,
                        trips=actual.trips or 0,
                        pipeline_run_id=config["pipeline_run_id"],
                        source_snapshot_ids=snapshot_ids,
                    )
                )
        return {**payload, "gold_metrics": counts}
    return execute_task(spark, config, "aggregate_gold", "dq_gate", action)


if __name__ == "__main__":
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
