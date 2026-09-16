"""Publish Databricks Gold exports to native BigQuery outside Serverless."""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from datetime import datetime, timedelta, timezone

from src.common.cost_guard import assert_confirmed, assert_non_production


def stage_table_name(mart: str, pipeline_run_id: str) -> str:
    safe_mart = mart.replace("-", "_")
    safe_run = pipeline_run_id.replace("-", "_")
    return f"_stage_{safe_mart}_{safe_run}"

ROOT = Path(__file__).resolve().parents[1]


def validate_handoff_target(handoff: dict, environment: str, project: str, dataset: str, bucket: str) -> None:
    if handoff.get("environment") != environment:
        raise ValueError("handoff environment does not match publisher environment")
    if handoff.get("project_id") != project or handoff.get("dataset") != dataset:
        raise ValueError("handoff BigQuery target does not match publisher target")
    prefix = f"gs://{bucket}/{environment}/gold_export/"
    for export in handoff.get("exports", []):
        if not export.get("uri", "").startswith(prefix):
            raise ValueError("handoff export URI is outside the expected environment prefix")


def load_handoff(path: str) -> dict:
    handoff = json.loads(Path(path).read_text(encoding="utf-8"))
    required = {
        "pipeline_run_id", "environment", "project_id", "dataset", "months",
        "gold_metrics", "exports", "status",
    }
    missing = required - set(handoff)
    if missing:
        raise ValueError(f"Serving handoff missing fields: {sorted(missing)}")
    if handoff["status"] != "READY_FOR_SERVING":
        raise ValueError(f"Serving handoff is not publishable: {handoff['status']}")
    expected_marts = set(TARGET_SCHEMAS)
    metrics = {(metric["mart"], metric["month"]) for metric in handoff["gold_metrics"]}
    exports = {(export["mart"], export["month"]) for export in handoff["exports"]}
    if metrics != exports or {mart for mart, _ in metrics} != expected_marts:
        raise ValueError("Handoff exports and metrics must cover every mart/month exactly once")
    if len(metrics) != len(handoff["gold_metrics"]) or len(exports) != len(handoff["exports"]):
        raise ValueError("Handoff contains duplicate mart/month entries")
    return handoff


MART_KEYS = {
    "revenue_by_zone_hour": (
        "pickup_month", "pickup_date", "pickup_hour", "pickup_location_id"
    ),
    "fare_integrity_daily": ("pickup_month", "pickup_date", "is_flat_fare"),
    "payment_mix_monthly": ("pickup_month", "payment_type_name"),
}


def publication_sql(handoff: dict, stages: dict[str, str]) -> str:
    project = handoff["project_id"]
    dataset = handoff["dataset"]
    marts = {metric["mart"] for metric in handoff["gold_metrics"]}
    if set(stages) != marts or marts != set(TARGET_SCHEMAS):
        raise ValueError("Publication stages must cover every Gold mart exactly once")

    statements = ["BEGIN TRANSACTION;"]
    for mart, stage in stages.items():
        target = f"`{project}.{dataset}.{mart}`"
        staged = f"`{project}.{dataset}.{stage}`"
        keys = MART_KEYS[mart]
        match = " AND ".join(f"T.{key} = S.{key}" for key in keys)
        columns = [name for name, _ in TARGET_SCHEMAS[mart]]
        expressions = {
            "avg_fare_amount": "CAST(S.avg_fare_amount AS NUMERIC)",
            "avg_trip_distance_mi": "CAST(S.avg_trip_distance_mi AS NUMERIC)",
        }
        assignments = ", ".join(
            f"T.{name} = {expressions.get(name, f'S.{name}')}" for name in columns
        )
        values = ", ".join(expressions.get(name, f"S.{name}") for name in columns)
        statements.append(
            f"MERGE {target} T USING {staged} S ON {match} "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({values});"
        )

    statements += [
        f"MERGE `{project}.{dataset}.publication_runs` T "
        "USING (SELECT @run_id AS pipeline_run_id, @environment AS environment, "
        "@months_json AS months_json, @snapshot_ids_json AS snapshot_ids_json, "
        "@metrics_json AS metrics_json, CURRENT_TIMESTAMP() AS published_at) S "
        "ON T.pipeline_run_id = S.pipeline_run_id "
        "WHEN MATCHED THEN UPDATE SET environment = S.environment, months_json = S.months_json, "
        "snapshot_ids_json = S.snapshot_ids_json, metrics_json = S.metrics_json, published_at = S.published_at "
        "WHEN NOT MATCHED THEN INSERT (pipeline_run_id, environment, months_json, snapshot_ids_json, metrics_json, published_at) "
        "VALUES (S.pipeline_run_id, S.environment, S.months_json, S.snapshot_ids_json, S.metrics_json, S.published_at);",
        "COMMIT TRANSACTION;",
    ]
    return "\n".join(statements)


TARGET_SCHEMAS = {
    "revenue_by_zone_hour": [
        ("pickup_month", "DATE"),
        ("pickup_date", "DATE"),
        ("pickup_hour", "INT64"),
        ("pickup_location_id", "INT64"),
        ("pickup_borough", "STRING"),
        ("pickup_zone", "STRING"),
        ("trip_count", "INT64"),
        ("total_revenue", "NUMERIC"),
        ("avg_fare_amount", "NUMERIC"),
        ("avg_trip_distance_mi", "NUMERIC"),
    ],
    "fare_integrity_daily": [
        ("pickup_month", "DATE"),
        ("pickup_date", "DATE"),
        ("is_flat_fare", "BOOL"),
        ("trip_count", "INT64"),
        ("avg_fare_per_mile", "FLOAT64"),
        ("fare_per_mile_p95", "FLOAT64"),
    ],
    "payment_mix_monthly": [
        ("pickup_month", "DATE"),
        ("payment_type_name", "STRING"),
        ("trip_count", "INT64"),
        ("pct_of_month_trips", "FLOAT64"),
        ("avg_tip_pct", "FLOAT64"),
    ],
}

PARQUET_SCHEMAS = {
    mart: [
        (name, "FLOAT64") if name in {"avg_fare_amount", "avg_trip_distance_mi"} else (name, field_type)
        for name, field_type in columns
    ]
    for mart, columns in TARGET_SCHEMAS.items()
}


def ensure_target_tables(client, project: str, dataset: str) -> None:
    for mart, columns in TARGET_SCHEMAS.items():
        table_id = f"{project}.{dataset}.{mart}"
        try:
            client.get_table(table_id)
        except NotFound:
            schema = [bigquery.SchemaField(name, field_type) for name, field_type in columns]
            table = bigquery.Table(table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(field="pickup_month")
            client.create_table(table)
    publication_id = f"{project}.{dataset}.publication_runs"
    try:
        client.get_table(publication_id)
    except NotFound:
        client.create_table(bigquery.Table(publication_id, schema=[
            bigquery.SchemaField("pipeline_run_id", "STRING"),
            bigquery.SchemaField("environment", "STRING"),
            bigquery.SchemaField("months_json", "STRING"),
            bigquery.SchemaField("snapshot_ids_json", "STRING"),
            bigquery.SchemaField("metrics_json", "STRING"),
            bigquery.SchemaField("published_at", "TIMESTAMP"),
        ]))


def publish_handoff(handoff: dict, client=None) -> dict:
    client = client or bigquery.Client(project=handoff["project_id"])
    project = handoff["project_id"]
    dataset = handoff["dataset"]
    if not handoff.get("exports"):
        raise ValueError("Serving handoff has no exported Gold files")
    ensure_target_tables(client, project, dataset)

    stages: dict[str, str] = {}
    attempt_id = uuid.uuid4().hex[:12]
    expiration = datetime.now(timezone.utc) + timedelta(days=2)
    seen: set[tuple[str, str]] = set()
    loaded_marts: set[str] = set()
    for export in handoff["exports"]:
        if export["month"] not in handoff["months"]:
            raise ValueError("Export month is outside the handoff affected-month set")
        mart = export["mart"]
        entry = (mart, export["month"])
        if entry in seen:
            raise ValueError(f"Duplicate handoff export: {mart}/{export['month']}")
        seen.add(entry)
        stage = stages.setdefault(
            mart, stage_table_name(mart, f"{handoff['pipeline_run_id']}_{attempt_id}")
        )
        table_ref = f"{project}.{dataset}.{stage}"
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if mart not in loaded_marts
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            autodetect=True,
        )
        load_job = client.load_table_from_uri(export["uri"] + "/*.parquet", table_ref, job_config=config)
        load_job.result()
        loaded_marts.add(mart)
        table = client.get_table(table_ref)
        table.expires = expiration
        client.update_table(table, ["expires"])

    query_job = client.query(
        publication_sql(handoff, stages),
        location="us-central1",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", handoff["pipeline_run_id"]),
            bigquery.ScalarQueryParameter("environment", "STRING", handoff["environment"]),
            bigquery.ScalarQueryParameter("months_json", "STRING", json.dumps(handoff["months"])),
            bigquery.ScalarQueryParameter(
                "snapshot_ids_json",
                "STRING",
                json.dumps(sorted({s["snapshot_id"] for s in handoff.get("snapshots", [])})),
            ),
            bigquery.ScalarQueryParameter("metrics_json", "STRING", json.dumps(handoff["gold_metrics"], sort_keys=True)),
        ]),
    )
    query_job.result()
    return {
        "status": "PUBLISHED",
        "pipeline_run_id": handoff["pipeline_run_id"],
        "job_id": query_job.job_id,
        "staging_tables": stages,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", required=True)
    parser.add_argument("--environment", choices=("dev", "staging"), required=True)
    parser.add_argument("--confirm-cost", action="store_true")
    parser.add_argument("--project", default="taxi-data-engineer")
    parser.add_argument("--dataset", default="taxi_analytics_staging")
    parser.add_argument("--bucket", default="taxi-data-engineer-taxi-lake-staging")
    args = parser.parse_args()
    assert_non_production(args.environment)
    assert_confirmed(args.confirm_cost)
    handoff = load_handoff(args.handoff)
    project = args.project
    dataset = args.dataset
    bucket = args.bucket
    validate_handoff_target(handoff, args.environment, project, dataset, bucket)
    print(json.dumps(publish_handoff(handoff), indent=2))


if __name__ == "__main__":
    main()
