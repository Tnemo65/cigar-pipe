"""Prepare a serving handoff from Gold for external native-BigQuery publication."""
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from google.cloud import bigquery
from pyspark.sql import functions as F
from src.common import paths
from src.common.runtime import month_start
from src.transform.run_gold_sql import MARTS


def _partition_export_path(options, config):
    if not options.get("datePartition"):
        raise ValueError("datePartition is required for a partition-scoped export")
    month = month_start(options["datePartition"])
    table = paths.identifier(options["table"].split(".")[-1])
    attempt = paths.identifier(options["attempt_id"])
    return (f"gs://{config['gcp']['bucket']}/{config['environment']}/gold_export/"
            f"{attempt}/{table}/pickup_month={month}")


def bq_schema(spark_schema):
    types = {"string": "STRING", "long": "INTEGER", "integer": "INTEGER",
             "short": "INTEGER", "double": "FLOAT", "float": "FLOAT",
             "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP",
             "timestamp_ntz": "DATETIME"}
    return [bigquery.SchemaField(paths.identifier(f.name), "NUMERIC" if f.dataType.typeName() == "decimal"
                                else types[f.dataType.typeName()]) for f in spark_schema]


def publication_sql(project, dataset, stages, months, run_id=None):
    paths.identifier(dataset)
    predicates = ", ".join(f"DATE '{month_start(m)}'" for m in months)
    if not predicates or set(stages) != set(MARTS):
        raise ValueError("Publication requires months and all marts")
    statements = ["BEGIN TRANSACTION;"]
    for mart in MARTS:
        paths.identifier(stages[mart])
        target, stage = f"`{project}.{dataset}.{mart}`", f"`{project}.{dataset}.{stages[mart]}`"
        statements += [f"DELETE FROM {target} WHERE pickup_month IN ({predicates});",
                       f"INSERT INTO {target} SELECT * FROM {stage};"]
    statements += [
        f"DELETE FROM `{project}.{dataset}.publication_runs` WHERE pipeline_run_id = @run_id;",
        f"INSERT INTO `{project}.{dataset}.publication_runs` "
        "(pipeline_run_id, environment, months_json, snapshot_ids_json, metrics_json, published_at) "
        "VALUES (@run_id, @environment, @months_json, @snapshot_ids_json, @metrics_json, CURRENT_TIMESTAMP());",
        "COMMIT TRANSACTION;",
    ]
    return "\n".join(statements)


def stage_expiration(config) -> timedelta:
    """Retain failed staging data long enough for diagnosis, not indefinitely."""
    return timedelta(days=7 if config["environment"] == "prod" else 2)


def publish(spark, config, payload, client=None):
    project, dataset = config["gcp"]["project_id"], config["bigquery"]["dataset"]
    client = client or bigquery.Client(project=project)
    location = config["gcp"]["region"]
    attempt = "run_" + uuid.uuid4().hex
    months = [month_start(m) for m in payload["months"]]
    if not months:
        raise RuntimeError("Missing publication months")
    stages, counts = {}, []
    for mart in MARTS:
        df = spark.table(paths.catalog_table("gold", mart, config)).filter(F.col("pickup_month").isin(months)).persist()
        try:
            schema = bq_schema(df.schema)
            target_id = f"{project}.{dataset}.{mart}"
            target = bigquery.Table(target_id, schema=schema)
            target.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="pickup_month")
            existing = client.create_table(target, exists_ok=True)
            if existing.table_type != "TABLE" or existing.time_partitioning is None or existing.time_partitioning.field != "pickup_month":
                raise RuntimeError(f"Serving table must be native and DATE-partitioned: {target_id}")
            if [(s.name, s.field_type) for s in existing.schema] != [(s.name, s.field_type) for s in schema]:
                raise RuntimeError(f"Serving schema migration required: {target_id}")
            stage_name = f"_stage_{mart}_{attempt}"
            stage_id = f"{project}.{dataset}.{stage_name}"
            stage = bigquery.Table(stage_id, schema=schema)
            stage.expires = datetime.now(timezone.utc) + stage_expiration(config)
            client.create_table(stage)
            total = df.count()
            for month in months:
                part = df.filter(F.col("pickup_month") == month)
                if not part.take(1):
                    continue
                uri = _partition_export_path(dict(table=mart, datePartition=month, attempt_id=attempt), config)
                part.write.mode("errorifexists").parquet(uri)
                client.load_table_from_uri(uri + "/*.parquet", stage_id, location=location,
                    job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                    schema=schema, write_disposition="WRITE_APPEND")).result()
            if client.get_table(stage_id).num_rows != total:
                raise RuntimeError(f"BigQuery load row reconciliation failed: {mart}")
            revenue_expr = ", SUM(total_revenue) AS revenue" if mart == "revenue_by_zone_hour" else ""
            actual = {str(r.pickup_month): r for r in client.query(
                f"SELECT pickup_month, COUNT(*) AS rows, SUM(trip_count) AS trips{revenue_expr} FROM `{stage_id}` GROUP BY pickup_month",
                location=location).result()}
            for month in months:
                aggs = [F.count("*").alias("rows"), F.sum("trip_count").alias("trips")]
                if revenue_expr:
                    aggs.append(F.sum("total_revenue").alias("revenue"))
                expected = df.filter(F.col("pickup_month") == month).agg(*aggs).first()
                loaded = actual.get(month)
                if (loaded.rows if loaded else 0) != expected.rows or (loaded.trips if loaded else 0) != (expected.trips or 0):
                    raise RuntimeError(f"BigQuery trip reconciliation failed: {mart}/{month}")
                if revenue_expr and (loaded.revenue if loaded else 0) != (expected.revenue or 0):
                    raise RuntimeError(f"BigQuery revenue reconciliation failed: {month}")
                snapshot_ids = [
                    snapshot["snapshot_id"]
                    for snapshot in payload.get("snapshots", [])
                    if snapshot["source_month"] == month
                ]
                counts.append(
                    dict(
                        mart=mart,
                        month=month,
                        rows=expected.rows,
                        trips=expected.trips or 0,
                        pipeline_run_id=config["pipeline_run_id"],
                        source_snapshot_ids=snapshot_ids,
                    )
                )

            stages[mart] = stage_name
        finally:
            df.unpersist()
    client.create_table(
        bigquery.Table(
            f"{project}.{dataset}.publication_runs",
            schema=[
                bigquery.SchemaField("pipeline_run_id", "STRING"),
                bigquery.SchemaField("environment", "STRING"),
                bigquery.SchemaField("months_json", "STRING"),
                bigquery.SchemaField("snapshot_ids_json", "STRING"),
                bigquery.SchemaField("metrics_json", "STRING"),
                bigquery.SchemaField("published_at", "TIMESTAMP"),
            ],
        ),
        exists_ok=True,
    )
    snapshot_ids = sorted({
        snapshot["snapshot_id"]
        for snapshot in payload.get("snapshots", [])
    })
    job = client.query(
        publication_sql(project, dataset, stages, months),
        location=location,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", config["pipeline_run_id"]),
            bigquery.ScalarQueryParameter("environment", "STRING", config["environment"]),
            bigquery.ScalarQueryParameter("months_json", "STRING", json.dumps(months)),
            bigquery.ScalarQueryParameter("snapshot_ids_json", "STRING", json.dumps(snapshot_ids)),
            bigquery.ScalarQueryParameter("metrics_json", "STRING", json.dumps(counts, sort_keys=True)),
        ]),
    )
    job.result()
    return {**payload, "publication_job_id": job.job_id, "serving_metrics": counts}


def prepare_serving_handoff(spark, config, payload):
    """Write versioned Gold files and return a publisher-ready manifest."""
    months = [month_start(month) for month in payload.get("months", [])]
    if not months:
        raise RuntimeError("Missing serving months")

    exports = []
    for mart in MARTS:
        table = spark.table(paths.catalog_table("gold", mart, config))
        for month in months:
            partition = table.filter(F.col("pickup_month") == month)
            if not partition.take(1):
                continue
            export_uri = (
                f"gs://{config['gcp']['bucket']}/{config['environment']}/"
                f"gold_export/{config['pipeline_run_id']}/{mart}/pickup_month={month}"
            )
            partition.write.mode("overwrite").parquet(export_uri)
            metrics = next(
                metric for metric in payload.get("gold_metrics", [])
                if metric["mart"] == mart and metric["month"] == month
            )
            exports.append({"mart": mart, "month": month, "uri": export_uri, "metrics": metrics})

    if len(exports) != len(payload.get("gold_metrics", [])):
        raise RuntimeError("Gold export did not produce one file set per mart/month metric")
    return {
        **payload,
        "serving_status": "READY_FOR_SERVING",
        "serving_handoff": {
            "pipeline_run_id": config["pipeline_run_id"],
            "environment": config["environment"],
            "project_id": config["gcp"]["project_id"],
            "dataset": config["bigquery"]["dataset"],
            "months": months,
            "snapshots": payload.get("snapshots", []),
            "exports": exports,
            "gold_metrics": payload.get("gold_metrics", []),
            "status": "READY_FOR_SERVING",
        },
    }


def run(spark, config, publisher=None):
    from src.common.run_state import execute_task
    action = (
        (lambda payload: publisher(spark, config, payload))
        if publisher is not None
        else (lambda payload: prepare_serving_handoff(spark, config, payload))
    )
    return execute_task(
        spark,
        config,
        "export_bigquery",
        "aggregate_gold",
        action,
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
