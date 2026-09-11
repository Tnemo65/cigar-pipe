"""Delta Lake Table Optimization and Maintenance Script.
Applies auto-compaction table properties and runs OPTIMIZE with Z-ORDER.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.databricks_sql import execute_statement

WAREHOUSE_ID = "d97366f8e702f01e"

TABLES_AUTO_OPTIMIZE = [
    "taxi_lakehouse.bronze.trips_raw",
    "taxi_lakehouse.silver.trips_clean",
    "taxi_lakehouse.silver.trips_quarantine",
    "taxi_lakehouse.gold.revenue_by_zone_hour",
    "taxi_lakehouse.gold.fare_integrity_daily",
    "taxi_lakehouse.gold.payment_mix_monthly",
]

STATEMENTS = [
    # 1. Enable Auto-Optimize (optimizeWrite + autoCompact)
    *[
        f"ALTER TABLE {tbl} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"
        for tbl in TABLES_AUTO_OPTIMIZE
    ],
    # 2. Z-ORDER Silver Clean by pickup timestamp and location for fast range/slice queries
    "OPTIMIZE taxi_lakehouse.silver.trips_clean ZORDER BY (pickup_at, pickup_location_id)",
    # 3. Z-ORDER Bronze Raw by pickup datetime
    "OPTIMIZE taxi_lakehouse.bronze.trips_raw ZORDER BY (tpep_pickup_datetime)",
]


def run_sql(stmt: str):
    print(f"Running: {stmt[:80]}...")
    execute_statement(stmt, WAREHOUSE_ID)
    print(" -> SUCCESS")


def main():
    for stmt in STATEMENTS:
        run_sql(stmt)
    print("All Delta tables successfully optimized and configured!")


if __name__ == "__main__":
    main()
