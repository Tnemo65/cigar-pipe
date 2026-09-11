-- Creates the pipeline run log table in the reference schema.
-- Run once during provisioning (idempotent).

CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.pipeline_run_log (
    task_name        STRING      NOT NULL,
    month            STRING      NOT NULL,
    status           STRING      NOT NULL,
    rows_processed   LONG,
    quarantine_rate  DOUBLE,
    error_message    STRING,
    logged_at        TIMESTAMP   NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 90 days'
);
