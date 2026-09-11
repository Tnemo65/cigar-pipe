# NYC Taxi Lakehouse

GCS → Bronze Delta (Auto Loader) → Silver (PySpark MERGE dedup) → Gold (SQL INSERT OVERWRITE) → BigQuery

## Architecture

```
GCS raw Parquet
      │
      ▼
Bronze (Delta, append-only, Auto Loader)
      │
      ▼
Silver (Delta, deduplicated via MERGE on trip_id SHA-256)
      │
      ▼
Gold marts (INSERT OVERWRITE by partition month)
      │
      ▼
BigQuery (export via Spark BigQuery connector)
```

## Setup

```bash
uv sync --group dev
```

## Run tests

```bash
uv run pytest tests/
```

## Deploy pipeline

```bash
databricks bundle deploy
databricks bundle run taxi_lakehouse_pipeline
```
