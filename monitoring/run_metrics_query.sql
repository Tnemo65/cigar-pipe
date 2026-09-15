-- Replace :catalog using the chosen environment; one durable row per run/task.
SELECT pipeline_run_id, task_name, status, updated_at,
       get_json_object(payload, '$.error') AS error,
       get_json_object(payload, '$.snapshots') AS source_snapshots,
       get_json_object(payload, '$.metrics') AS silver_metrics,
       get_json_object(payload, '$.gold_metrics') AS gold_metrics,
       get_json_object(payload, '$.serving_metrics') AS serving_metrics,
       get_json_object(payload, '$.unavailable_months') AS unavailable_months,
       get_json_object(payload, '$.cost_context.duration_ms') AS task_duration_ms,
       get_json_object(payload, '$.cost_context.environment') AS environment,
       get_json_object(payload, '$.cost_context.backfill') AS is_backfill
FROM :catalog.reference.pipeline_run_state
ORDER BY updated_at DESC;

-- Schedule independently of the pipeline to detect a missing daily job.
SELECT CASE WHEN max(updated_at) < current_timestamp() - INTERVAL 26 HOURS
                 OR max(updated_at) IS NULL THEN 'ALERT: no completed source check'
            ELSE 'OK' END AS freshness_status
FROM :catalog.reference.pipeline_run_state
WHERE task_name = 'source_landing' AND status IN ('SUCCESS', 'NO_DATA');
