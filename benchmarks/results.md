# Benchmark Results

## 1. Zone-driven data skew (design.md §9.1)

| Tier | Baseline (s) | Salted (s) | Max/median task ratio (baseline) | Max/median task ratio (salted) |
|---|---|---|---|---|
| Medium | <fill in> | <fill in> | <fill in from Spark UI> | <fill in from Spark UI> |
| Large  | <fill in> | <fill in> | <fill in from Spark UI> | <fill in from Spark UI> |

## 2. Join-strategy proof: broadcast vs sort-merge (design.md §9.2)

Procedure: run `sql/gold/revenue_by_zone_hour.sql`'s query once with
`spark.sql.autoBroadcastJoinThreshold` at its default, once with it set to
`-1` (forces sort-merge). Record shuffle read/write bytes and stage
duration from the Spark UI's SQL tab for both runs.

| Tier | Broadcast: stage duration | Broadcast: shuffle bytes | Sort-merge: stage duration | Sort-merge: shuffle bytes |
|---|---|---|---|---|
| Medium | <fill in> | <fill in> | <fill in> | <fill in> |
| Large  | <fill in> | <fill in> | <fill in> | <fill in> |

## 3. Incremental vs full-recompute Gold refresh (design.md §9.3)

Procedure: at the large tier, time `run_gold_sql.py` for one new month (a)
scoped normally (the static-partition `INSERT OVERWRITE ... PARTITION`
already implemented) vs (b) a temporary full-history variant with the
`PARTITION (pickup_month = :month)` clause and `WHERE t.pickup_month =
:month` filter both removed, aggregating all of Silver history instead.

| Metric | Incremental (1 new month) | Full recompute |
|---|---|---|
| Wall-clock | <fill in> | <fill in> |
| Bytes scanned | <fill in> | <fill in> |
