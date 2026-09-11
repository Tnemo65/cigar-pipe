-- sql/reference/pipeline_run_log.sql
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.pipeline_run_log (
  task_name        STRING    NOT NULL,
  rows_in          BIGINT,
  rows_out         BIGINT,
  rows_quarantined BIGINT,
  status           STRING    NOT NULL,
  started_at       TIMESTAMP NOT NULL,
  ended_at         TIMESTAMP NOT NULL
) USING DELTA;
