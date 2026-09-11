-- Immutable source landing and replay ledger.
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.ingestion_manifest (
  source_system       STRING NOT NULL,
  dataset_name        STRING NOT NULL,
  snapshot_id         STRING NOT NULL,
  object_name         STRING NOT NULL,
  object_generation   STRING,
  object_checksum     STRING NOT NULL,
  request_id          STRING,
  page_cursor         STRING,
  page_number         INT,
  status              STRING NOT NULL,
  rows_received       BIGINT,
  pipeline_run_id     STRING,
  first_seen_at       TIMESTAMP NOT NULL,
  completed_at        TIMESTAMP,
  error_message       STRING
) USING DELTA;
