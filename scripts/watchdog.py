"""Independent hourly check: detects when the daily pipeline never starts."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from src.common import paths
from src.common.runtime import configure_spark, runtime_config


def run(spark, config):
    table = paths.catalog_table("reference", "pipeline_run_state", config)
    latest = (spark.table(table).filter((F.col("task_name") == "source_landing") &
              F.col("status").isin("SUCCESS", "NO_DATA"))
              .agg(F.max("updated_at").alias("last_check")).first().last_check)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if latest is None or latest < now - timedelta(hours=26):
        raise RuntimeError("Freshness alert: no successful source check in 26 hours")
    print(f"Source check last completed at {latest} UTC")


if __name__ == "__main__":
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
