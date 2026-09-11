-- Recent pipeline run summary: last 30 days, one row per task+month+status.
-- Use in Databricks SQL or a BI tool to monitor pipeline health.

SELECT
    task_name,
    month,
    status,
    rows_processed,
    ROUND(quarantine_rate * 100, 2)  AS quarantine_rate_pct,
    error_message,
    logged_at
FROM taxi_lakehouse.reference.pipeline_run_log
WHERE logged_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
ORDER BY logged_at DESC
LIMIT 200;
