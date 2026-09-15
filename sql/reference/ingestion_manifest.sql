CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.ingestion_manifest (
  pipeline_run_id STRING NOT NULL,
  source_month DATE NOT NULL,
  source_snapshot_id STRING NOT NULL,
  source_url STRING,
  object_uri STRING NOT NULL,
  object_name STRING NOT NULL,
  object_checksum STRING NOT NULL,
  object_generation STRING,
  source_version STRING,
  rows_received BIGINT NOT NULL,
  bytes_received BIGINT,
  status STRING NOT NULL,
  first_seen_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  metadata_json STRING
) USING DELTA;
