-- sql/reference/pipeline_run_log.sql
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.pipeline_run_log (
  pipeline_run_id   STRING,
  task_name        STRING    NOT NULL,
  batch_id         STRING,
  source_snapshot_id STRING,
  rows_in          BIGINT,
  rows_out         BIGINT,
  rows_deduplicated BIGINT,
  rows_quarantined BIGINT,
  status           STRING    NOT NULL,
  started_at       TIMESTAMP NOT NULL,
  ended_at         TIMESTAMP NOT NULL
) USING DELTA;
