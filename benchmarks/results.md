# Benchmark Results

These results are historical observations, not a production performance guarantee. The
benchmark runner records **client elapsed time** after the Databricks SQL statement
reaches a terminal state; this includes API, network, warehouse queue, and query time.
Execution-stage metrics and shuffle metrics require Databricks query profiles and are
not produced by the local runner.

## 1. Zone-driven data skew

| Dataset | Baseline | Salted | Interpretation |
|---|---:|---:|---|
| 2.7M rows, historical single run | 2.02s | 1.80s | Historical observation; repeat before using as a capacity claim |
| Multi-month | Not measured | Not measured | No projection is presented as a measured result |

The salted query uses eight buckets from `pmod(hash(trip_id), 8)`. Salting adds a
second aggregation and is only beneficial when the input distribution actually has a
problematic hot key. It should be selected from query-profile evidence, not assumed
for every workload.

## 2. Join strategy

The historical single-run observation was:

| Strategy | Historical client elapsed | Interpretation |
|---|---:|---|
| Broadcast | 4.73s | Dimension-side shuffle avoided in the observed plan |
| Sort-merge | 3.23s | Faster in this observed run despite shuffle |

Zero dimension shuffle does not prove lower wall-clock time. Repeat the comparison with
warm-up, multiple measurements, identical cache state, and query-profile metrics before
changing a production join strategy.

## 3. Incremental versus full read

The current benchmark runner measures read queries, not Gold refresh writes:

| Query | Historical client elapsed | Scope |
|---|---:|---|
| Partition-filtered read | 3.58s | One month |
| Full-history read | 3.55s | All available history |

These numbers do **not** prove `INSERT OVERWRITE` duration, Delta commit behavior,
partition replacement, or write idempotency. A valid refresh benchmark must execute the
actual write against an isolated benchmark target and record the Databricks operation
profile.
