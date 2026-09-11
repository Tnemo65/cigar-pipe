-- monitoring/run_metrics_query.sql
-- Ad-hoc health check: last 20 runs per task, with duration and quarantine rate.
SELECT
  task_name,
  status,
  rows_in, rows_out, rows_quarantined,
  round(rows_quarantined / NULLIF(rows_in, 0) * 100, 2) AS quarantine_pct,
  started_at, ended_at,
  (unix_timestamp(ended_at) - unix_timestamp(started_at)) AS duration_seconds
FROM taxi_lakehouse.reference.pipeline_run_log
ORDER BY started_at DESC
LIMIT 20;
