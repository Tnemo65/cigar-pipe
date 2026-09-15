CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.pipeline_run_state (
  pipeline_run_id STRING, task_name STRING, status STRING,
  payload STRING, updated_at TIMESTAMP
) USING DELTA;
