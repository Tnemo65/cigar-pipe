"""Comprehensive End-to-End Verification Report Script.
Queries BigQuery and Databricks SQL Warehouse to print verified numbers for presentation.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml
from google.cloud import bigquery

from scripts.databricks_sql import execute_statement

CONFIG_PATH = ROOT / "configs" / "config.yaml"
PROJECT_ID = "taxi-data-engineer"
DATASET_ID = "taxi_analytics"
WAREHOUSE_ID = "d97366f8e702f01e"


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def query_databricks(stmt: str):
    response = execute_statement(stmt, WAREHOUSE_ID)
    return response.get("result", {}).get("data_array", [])


def assert_quarantine_rate(raw_count: int, quarantine_count: int, threshold: float) -> float:
    if raw_count <= 0:
        raise ValueError("raw row count must be positive")
    rate = quarantine_count / raw_count
    if rate > threshold:
        raise AssertionError(
            f"quarantine rate {rate:.4f} exceeds threshold {threshold:.4f}"
        )
    return rate

def main() -> None:
    print("================================================================")
    print("          NYC TAXI LAKEHOUSE PRODUCTION VERIFICATION REPORT      ")
    print("================================================================\n")

    config = load_config()
    threshold = float(config["thresholds"]["quarantine_rate_max"])

    print("--- 1. DATABRICKS DELTA LAKEHOUSE ROW COUNTS ---")
    db_tables = [
        "taxi_lakehouse.bronze.trips_raw",
        "taxi_lakehouse.silver.trips_clean",
        "taxi_lakehouse.silver.trips_quarantine",
        "taxi_lakehouse.gold.revenue_by_zone_hour",
        "taxi_lakehouse.gold.fare_integrity_daily",
        "taxi_lakehouse.gold.payment_mix_monthly",
        "taxi_lakehouse.reference.dim_zone",
        "taxi_lakehouse.reference.dim_rate_code",
        "taxi_lakehouse.reference.dim_payment_type",
    ]
    for table in db_tables:
        count = query_databricks(f"SELECT count(*) FROM {table}")
        print(f"  {table:<45}: {int(count[0][0]):>10,d} rows")

    print("\n--- 2. GOOGLE BIGQUERY EXTERNAL (BIGLAKE) TABLES ---")
    client = bigquery.Client(project=PROJECT_ID)
    revenue_result = list(client.query(
        f"SELECT count(*) AS cnt, sum(trip_count) AS trips, "
        f"round(sum(total_revenue), 2) AS rev FROM "
        f"`{PROJECT_ID}.{DATASET_ID}.revenue_by_zone_hour`"
    ).result())[0]
    print(
        f"  revenue_by_zone_hour : {revenue_result.cnt:>8,d} rows | "
        f"{revenue_result.trips:>10,d} trips | ${revenue_result.rev:>14,f} total revenue"
    )

    fare_result = list(client.query(
        f"SELECT count(*) AS cnt, sum(trip_count) AS trips, "
        f"round(avg(avg_fare_per_mile), 2) AS fpm FROM "
        f"`{PROJECT_ID}.{DATASET_ID}.fare_integrity_daily`"
    ).result())[0]
    print(
        f"  fare_integrity_daily : {fare_result.cnt:>8,d} rows | "
        f"{fare_result.trips:>10,d} trips | ${fare_result.fpm:>6.2f}/mile avg fare"
    )

    payment_results = list(client.query(
        f"SELECT payment_type_name, trip_count, "
        f"round(pct_of_month_trips * 100, 2) AS pct, "
        f"round(avg_tip_pct * 100, 2) AS tip FROM "
        f"`{PROJECT_ID}.{DATASET_ID}.payment_mix_monthly` "
        "ORDER BY trip_count DESC"
    ).result())
    print(f"  payment_mix_monthly  : {len(payment_results):>8,d} categories:")
    for row in payment_results:
        tip = f"{row.tip:>5.1f}%" if row.tip is not None else " N/A "
        print(
            f"    - {row.payment_type_name:<15}: {row.trip_count:>10,d} trips "
            f"({row.pct:>5.2f}%) | Avg Tip: {tip}"
        )

    print("\n--- 3. DATA QUALITY GOVERNANCE ---")
    raw_count = int(query_databricks(
        "SELECT count(*) FROM taxi_lakehouse.bronze.trips_raw"
    )[0][0])
    clean_count = int(query_databricks(
        "SELECT count(*) FROM taxi_lakehouse.silver.trips_clean"
    )[0][0])
    quarantine_count = int(query_databricks(
        "SELECT count(*) FROM taxi_lakehouse.silver.trips_quarantine"
    )[0][0])
    if clean_count + quarantine_count > raw_count:
        raise AssertionError(
            "Silver clean plus quarantine exceeds Bronze; duplicate or retry data detected"
        )
    deduplicated_rows = raw_count - clean_count - quarantine_count
    rate = assert_quarantine_rate(raw_count, quarantine_count, threshold)
    print(f"  Total Ingested   : {raw_count:>10,d}")
    print(f"  Valid (Clean)    : {clean_count:>10,d} ({clean_count/raw_count*100:>5.2f}%)")
    print(f"  Quarantined      : {quarantine_count:>10,d} ({rate*100:>5.2f}%)")
    print(f"  Deduplicated     : {deduplicated_rows:>10,d} Bronze rows not re-emitted")
    print(f"  Threshold Status : PASS ({rate*100:.2f}% <= {threshold*100:.2f}%)")

    print("\n================================================================")
    print("                    ALL CHECKS PASSED                         ")
    print("================================================================\n")

if __name__ == "__main__":
    main()
