from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery
from pyspark.sql import functions as F
from src.export.export_bigquery import _partition_export_path, publication_sql, publish, run, stage_expiration
from src.transform.run_gold_sql import MARTS

CFG = {"environment": "staging", "pipeline_run_id": "123",
       "databricks": {"catalog": "taxi_lakehouse_staging"},
       "gcp": {"project_id": "test-project", "bucket": "lake-staging", "region": "us-central1"},
       "bigquery": {"dataset": "taxi_staging"}}


def test_export_uses_private_versioned_iso_date_path():
    uri = _partition_export_path({"table": "dataset.revenue_by_zone_hour", "datePartition": "2024-01-01", "attempt_id": "run_abc"}, CFG)
    assert uri == "gs://lake-staging/staging/gold_export/run_abc/revenue_by_zone_hour/pickup_month=2024-01-01"
    with pytest.raises(ValueError):
        _partition_export_path({"table": "x", "datePartition": "20240101", "attempt_id": "run_abc"}, CFG)


def test_staging_expiration_is_shorter_outside_production():
    assert stage_expiration(CFG).days == 2
    assert stage_expiration({**CFG, "environment": "prod"}).days == 7


def test_all_marts_and_receipt_share_one_transaction():
    sql = publication_sql("test-project", "taxi_staging", {m: "stage_"+m for m in MARTS}, ["2024-01-01", "2024-02-01"])
    assert sql.startswith("BEGIN TRANSACTION;") and sql.endswith("COMMIT TRANSACTION;")
    assert sql.count("DELETE FROM") == 4
    assert "pickup_month IN (DATE '2024-01-01', DATE '2024-02-01')" in sql
    assert "@run_id" in sql
    assert "@snapshot_ids_json" in sql
    with pytest.raises(ValueError):
        publication_sql("test-project", "taxi_staging", {}, ["2024-01-01"])


class FakeClient:
    def __init__(self, rows, fail_load=False):
        self.tables, self.sql, self.rows = {}, [], rows
        self.fail_load = fail_load
    def create_table(self, table, exists_ok=False):
        table._properties["type"] = "TABLE"
        self.tables[table.table_id] = table
        return table
    def get_table(self, name):
        return SimpleNamespace(num_rows=len(self.rows))
    def load_table_from_uri(self, uri, table, **kwargs):
        job = MagicMock()
        if self.fail_load:
            job.result.side_effect = RuntimeError("load failed")
        return job
    def query(self, sql, **kwargs):
        self.sql.append(sql)
        job = MagicMock(job_id="bq-job-1")
        job.result.return_value = self.rows if sql.startswith("SELECT") else []
        return job


@pytest.mark.parametrize("empty", [False, True])
def test_publication_loads_then_commits_even_empty_partitions(spark, empty):
    df = spark.createDataFrame([(date(2024,1,1), 3)], "pickup_month DATE, trip_count LONG").withColumn("total_revenue", F.lit(12).cast("decimal(12,2)"))
    if empty:
        df = df.limit(0)
    client = FakeClient([] if empty else [SimpleNamespace(pickup_month=date(2024,1,1), rows=1, trips=3, revenue=12)])
    with patch.object(spark, "table", return_value=df), patch("pyspark.sql.readwriter.DataFrameWriter.parquet") as write:
        result = publish(spark, CFG, {"months":["2024-01-01"]}, client)
    assert result["publication_job_id"] == "bq-job-1"
    assert client.sql[-1].startswith("BEGIN TRANSACTION;")
    assert write.call_count == (0 if empty else 3)


def test_failed_load_never_publishes(spark):
    df = spark.createDataFrame([(date(2024,1,1), 3)], "pickup_month DATE, trip_count LONG")
    client = FakeClient([], fail_load=True)
    with patch.object(spark, "table", return_value=df), patch("pyspark.sql.readwriter.DataFrameWriter.parquet"):
        with pytest.raises(RuntimeError, match="load failed"):
            publish(spark, CFG, {"months":["2024-01-01"]}, client)
    assert not any("BEGIN TRANSACTION" in sql for sql in client.sql)


def test_runtime_export_reads_persistent_state_without_dbutils():
    payload = {"status":"SUCCESS", "snapshots":[{"snapshot_id":"x"}], "months":["2024-01-01"]}
    publisher = MagicMock(return_value=payload)
    with patch("src.common.run_state.RunState") as cls:
        cls.return_value.read.return_value = payload
        run(MagicMock(), CFG, publisher)
    publisher.assert_called_once()
    assert cls.return_value.read.call_args.args == ("aggregate_gold",)
