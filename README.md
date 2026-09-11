# NYC Taxi Lakehouse

GCS → Bronze Delta (Auto Loader) → Silver (PySpark MERGE dedup) → Gold (SQL INSERT OVERWRITE) → GCS Parquet partitions → BigLake external tables → BigQuery

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
GCS Parquet (one Hive partition per touched month)
      │
      ▼
BigLake external tables → BigQuery
```

## Setup

```bash
uv sync --group dev
```

## Run tests

```bash
uv run pytest tests/
```

## Operations

See `docs/operations.md` for delivery semantics, replay invariants, daily operation,
scale-test tiers, CI/CD promotion, and recovery requirements.

## Deploy pipeline

```bash
databricks bundle deploy
databricks bundle run taxi_pipeline
```
