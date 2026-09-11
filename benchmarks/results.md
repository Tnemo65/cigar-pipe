# Skew Benchmark Results

## Setup

- Silver table: `taxi_lakehouse.silver.yellow_trips`
- Grouped by: `pu_location_id`
- Salt buckets: 8
- Cluster: _fill in node type and worker count_

## Results

| Month | Strategy | Rows | Time (s) | Speedup |
|-------|----------|------|----------|---------|
| YYYY-MM | baseline | — | — | 1.00x |
| YYYY-MM | salted_8 | — | — | —x |

## Notes

- Run `python -m src.transform.skew_benchmark --month YYYY-MM` on the ETL cluster.
- Replace the placeholder rows above with actual output from the script.
- Speedup varies by data volume and cluster size; re-run after any cluster resize.
