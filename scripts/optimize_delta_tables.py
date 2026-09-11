"""Delta Lake Table Optimization and Maintenance Script.
Applies auto-compaction table properties and runs OPTIMIZE with Z-ORDER.
"""
import json
import subprocess
import time

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
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": stmt,
        "wait_timeout": "50s",
    })

    with open("temp_opt_payload.json", "w") as f:
        f.write(payload)

    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", "@temp_opt_payload.json"],
        capture_output=True,
        text=True,
        check=True,
    )
    res = json.loads(proc.stdout)
    state = res.get("status", {}).get("state")
    if state not in ("SUCCEEDED", "CLOSED"):
        raise RuntimeError(f"SQL statement failed ({state}): {res}")
    print(" -> SUCCESS")


def main():
    for stmt in STATEMENTS:
        run_sql(stmt)
    # Clean up temp file
    import os
    if os.path.exists("temp_opt_payload.json"):
        os.remove("temp_opt_payload.json")
    print("All Delta tables successfully optimized and configured!")


if __name__ == "__main__":
    main()
