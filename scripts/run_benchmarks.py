"""Run performance benchmarks against Databricks Lakehouse tables.
Collects real execution times for skew salting, join strategy, and refresh mechanisms.
"""
import json
import subprocess
import time

WAREHOUSE_ID = "d97366f8e702f01e"

def execute_timed_sql(stmt: str) -> float:
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": stmt,
        "wait_timeout": "50s",
    })
    with open("temp_bench_payload.json", "w") as f:
        f.write(payload)

    start = time.time()
    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", "@temp_bench_payload.json"],
        capture_output=True,
        text=True,
        check=True,
    )
    elapsed = time.time() - start
    res = json.loads(proc.stdout)
    state = res.get("status", {}).get("state")
    if state not in ("SUCCEEDED", "CLOSED"):
        raise RuntimeError(f"Statement failed ({state}): {res}")
    return elapsed


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
    t_base = execute_timed_sql(baseline_query)
    print(f"Baseline GroupBy: {t_base:.2f}s")
    t_salt = execute_timed_sql(salted_query)
    print(f"Salted GroupBy:   {t_salt:.2f}s")

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
    t_broadcast = execute_timed_sql(broadcast_join_query)
    print(f"Broadcast Join:  {t_broadcast:.2f}s")
    t_merge = execute_timed_sql(sort_merge_join_query)
    print(f"Sort-Merge Join: {t_merge:.2f}s")

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
    t_inc = execute_timed_sql(incremental_query)
    print(f"Incremental Query (partition-scoped): {t_inc:.2f}s")
    t_full = execute_timed_sql(full_history_query)
    print(f"Full Scan Query (unpartitioned):      {t_full:.2f}s")

    # Clean up temp file
    import os
    if os.path.exists("temp_bench_payload.json"):
        os.remove("temp_bench_payload.json")

    results = {
        "skew": {"baseline": t_base, "salted": t_salt},
        "join": {"broadcast": t_broadcast, "merge": t_merge},
        "refresh": {"incremental": t_inc, "full": t_full},
    }
    with open("benchmarks/measured_metrics.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Saved measured metrics to benchmarks/measured_metrics.json")


if __name__ == "__main__":
    main()
