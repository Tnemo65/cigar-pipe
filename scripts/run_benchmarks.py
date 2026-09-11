"""Run performance benchmarks against Databricks Lakehouse tables.
Collects client elapsed times after statements reach a terminal state.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.databricks_sql import execute_statement

WAREHOUSE_ID = "d97366f8e702f01e"


def execute_timed_sql(stmt: str) -> dict:
    """Return completion-aware client timing and statement metadata."""
    start = time.monotonic()
    response = execute_statement(stmt, WAREHOUSE_ID)
    return {
        "client_elapsed_seconds": time.monotonic() - start,
        "statement_id": response.get("statement_id"),
    }


def measure_query(stmt: str, repetitions: int = 3) -> dict:
    """Warm up once, then collect repeated client timings for one statement."""
    execute_timed_sql(stmt)
    measurements = [execute_timed_sql(stmt) for _ in range(repetitions)]
    elapsed = sorted(item["client_elapsed_seconds"] for item in measurements)
    median = elapsed[len(elapsed) // 2]
    return {
        "runs": measurements,
        "min_client_elapsed_seconds": min(elapsed),
        "median_client_elapsed_seconds": median,
        "max_client_elapsed_seconds": max(elapsed),
    }


def main():
    print("=== 1. SKEW BENCHMARK ===")
    baseline_query = """
    SELECT pickup_location_id, hour(pickup_at) as pickup_hour, count(*) as cnt
    FROM taxi_lakehouse.silver.trips_clean
    GROUP BY pickup_location_id, hour(pickup_at)
    """
    salted_query = """
    WITH salted AS (
      SELECT pickup_location_id, hour(pickup_at) as pickup_hour, (pmod(hash(trip_id), 8)) as _salt, count(*) as cnt
      FROM taxi_lakehouse.silver.trips_clean
      GROUP BY pickup_location_id, hour(pickup_at), (pmod(hash(trip_id), 8))
    )
    SELECT pickup_location_id, pickup_hour, sum(cnt) as cnt
    FROM salted
    GROUP BY pickup_location_id, pickup_hour
    """
    t_base = measure_query(baseline_query)
    print(f"Baseline GroupBy median: {t_base['median_client_elapsed_seconds']:.2f}s")
    t_salt = measure_query(salted_query)
    print(f"Salted GroupBy median:   {t_salt['median_client_elapsed_seconds']:.2f}s")

    print("\n=== 2. JOIN STRATEGY BENCHMARK ===")
    broadcast_join_query = """
    SELECT /*+ BROADCAST(z) */
           date_trunc('hour', t.pickup_at) as pickup_hour,
           t.pickup_location_id,
           z.zone as zone_name,
           z.borough,
           count(*) as trip_count,
           sum(t.total_amount) as total_revenue
    FROM taxi_lakehouse.silver.trips_clean t
    JOIN taxi_lakehouse.reference.dim_zone z
      ON t.pickup_location_id = z.location_id
    WHERE t.pickup_month = '2024-01-01'
    GROUP BY date_trunc('hour', t.pickup_at), t.pickup_location_id, z.zone, z.borough
    """
    sort_merge_join_query = """
    SELECT /*+ SHUFFLE_MERGE(z) */
           date_trunc('hour', t.pickup_at) as pickup_hour,
           t.pickup_location_id,
           z.zone as zone_name,
           z.borough,
           count(*) as trip_count,
           sum(t.total_amount) as total_revenue
    FROM taxi_lakehouse.silver.trips_clean t
    JOIN taxi_lakehouse.reference.dim_zone z
      ON t.pickup_location_id = z.location_id
    WHERE t.pickup_month = '2024-01-01'
    GROUP BY date_trunc('hour', t.pickup_at), t.pickup_location_id, z.zone, z.borough
    """
    t_broadcast = measure_query(broadcast_join_query)
    print(f"Broadcast Join median:  {t_broadcast['median_client_elapsed_seconds']:.2f}s")
    t_merge = measure_query(sort_merge_join_query)
    print(f"Sort-Merge Join median: {t_merge['median_client_elapsed_seconds']:.2f}s")

    print("\n=== 3. INCREMENTAL VS FULL RECOMPUTE ===")
    incremental_query = """
    SELECT date_trunc('hour', t.pickup_at) as pickup_hour,
           t.pickup_location_id,
           count(*) as trip_count,
           sum(t.total_amount) as total_revenue
    FROM taxi_lakehouse.silver.trips_clean t
    WHERE t.pickup_month = '2024-01-01'
    GROUP BY date_trunc('hour', t.pickup_at), t.pickup_location_id
    """
    full_history_query = """
    SELECT date_trunc('hour', t.pickup_at) as pickup_hour,
           t.pickup_location_id,
           count(*) as trip_count,
           sum(t.total_amount) as total_revenue
    FROM taxi_lakehouse.silver.trips_clean t
    GROUP BY date_trunc('hour', t.pickup_at), t.pickup_location_id
    """
    t_inc = measure_query(incremental_query)
    print(
        f"Incremental read median (partition-scoped): "
        f"{t_inc['median_client_elapsed_seconds']:.2f}s"
    )
    t_full = measure_query(full_history_query)
    print(
        f"Full read median (unpartitioned):          "
        f"{t_full['median_client_elapsed_seconds']:.2f}s"
    )

    results = {
        "measurement_scope": "client elapsed time including API and warehouse wait",
        "repetitions": 3,
        "skew": {"baseline": t_base, "salted": t_salt},
        "join": {"broadcast": t_broadcast, "merge": t_merge},
        "refresh": {
            "incremental_read": t_inc,
            "full_history_read": t_full,
            "note": "These are read benchmarks, not INSERT OVERWRITE refresh benchmarks.",
        },
    }
    with open("benchmarks/measured_metrics.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Saved measured metrics to benchmarks/measured_metrics.json")


if __name__ == "__main__":
    main()
