-- A partition is serving-visible only after a COMMITTED record exists.
CREATE TABLE IF NOT EXISTS taxi_lakehouse.reference.gold_publication_manifest (
  table_name       STRING NOT NULL,
  partition_name   STRING NOT NULL,
  pipeline_run_id  STRING NOT NULL,
  source_snapshot_id STRING,
  delta_version    BIGINT,
  row_count        BIGINT NOT NULL,
  schema_hash      STRING,
  export_path      STRING NOT NULL,
  status           STRING NOT NULL,
  published_at     TIMESTAMP NOT NULL,
  error_message    STRING
) USING DELTA;
