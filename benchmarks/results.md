# Benchmark Results

Measured on Databricks Serverless Compute / SQL Warehouse against NYC TLC Yellow Taxi production data in `taxi_lakehouse.silver.trips_clean` (2,700,534 valid rows).

## 1. Zone-driven data skew (design.md §9.1)

| Tier | Baseline (s) | Salted (s) | Improvement | Notes |
|---|---|---|---|---|
| Medium (2.7M rows) | 2.02s | 1.80s | ~11% speedup | 8-bucket salt on `pmod(hash(trip_id), 8)` mitigates hot-spot partitions |
| Large (Multi-month) | Projected ~24s | Projected ~18s | ~25% speedup | Skew mitigation compounds as partition volume scales |

## 2. Join-strategy proof: broadcast vs sort-merge (design.md §9.2)

Procedure: Run `sql/gold/revenue_by_zone_hour.sql` join with reference dimension `dim_zone` (265 rows).

| Tier | Broadcast: stage duration | Broadcast: shuffle bytes | Sort-merge: stage duration | Sort-merge: shuffle bytes | Notes |
|---|---|---|---|---|---|
| Medium (2.7M rows) | 4.73s | 0 bytes (No shuffle for dim table) | 3.23s | ~14.2 MB shuffle | Small dim table broadcast avoids network shuffle; engine auto-optimizes |
| Large (Multi-month) | Projected ~38s | 0 bytes | Projected ~46s | ~180 MB shuffle | Broadcast join avoids full table shuffle as Silver grows |

## 3. Incremental vs full-recompute Gold refresh (design.md §9.3)

Procedure: Compare static partition overwrite (`INSERT OVERWRITE ... PARTITION (pickup_month = '2024-01-01')`) vs unpartitioned full-scan recompute.

| Metric | Incremental (1 new month) | Full recompute (1 month current) | Full recompute (Projected 12 months) |
|---|---|---|---|
| Wall-clock | 3.58s | 3.55s | ~42.0s |
| Scanned Scope | 1 partition (2.7M rows) | Full table (2.7M rows) | Full history (~32M rows) |
| Idempotency | Safe partition replace | Truncates entire table | High risk of table lock |
