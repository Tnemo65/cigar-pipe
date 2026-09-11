"""Setup BigQuery External Tables over GCS Gold Mart exports."""
import sys
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = "taxi-data-engineer"
DATASET_ID = "taxi_analytics"

DDLS = [
    f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.revenue_by_zone_hour`
    WITH PARTITION COLUMNS (
      pickup_month INT64
    )
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/revenue_by_zone_hour/*'],
      hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/revenue_by_zone_hour'
    )
    """,
    f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.fare_integrity_daily`
    WITH PARTITION COLUMNS (
      pickup_month INT64
    )
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/fare_integrity_daily/*'],
      hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/fare_integrity_daily'
    )
    """,
    f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.payment_mix_monthly`
    WITH PARTITION COLUMNS (
      pickup_month INT64
    )
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/payment_mix_monthly/*'],
      hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/payment_mix_monthly'
    )
    """,
]


def setup_external_tables():
    """Provision external tables without a destructive DROP step."""
    client = bigquery.Client(project=PROJECT_ID)

    for ddl in DDLS:
        table_name = ddl.split("EXTERNAL TABLE")[1].split("WITH")[0].strip()
        print(f"Creating External Table {table_name}...")
        query_job = client.query(ddl)
        query_job.result()
        print(f" -> {table_name} created successfully.")

    # Verify queryability
    print("\nVerifying queryability on BigQuery External Tables:")
    check_query = f"SELECT count(*) as total_rows, sum(trip_count) as total_trips, round(sum(total_revenue), 2) as total_revenue FROM `{PROJECT_ID}.{DATASET_ID}.revenue_by_zone_hour`"
    res = client.query(check_query).result()
    for row in res:
        print(f" -> revenue_by_zone_hour: rows={row.total_rows}, trips={row.total_trips}, rev=${row.total_revenue}")


if __name__ == "__main__":
    setup_external_tables()
