# tests/test_export_bigquery.py
from unittest.mock import MagicMock

from src.export.export_bigquery import _partition_export_path, export_month


def test_export_month_filters_to_the_right_partition_and_calls_writer(spark):
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-02-01", 2)], ["pickup_month", "trip_count"]
    ).withColumn("pickup_month", __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date("pickup_month"))
    df.createOrReplaceTempView("gold_test_table")

    writer = MagicMock()
    export_month(
        spark,
        gold_table="gold_test_table",
        bq_dataset="taxi_analytics",
        month="2024-01-01",
        writer=writer,
    )

    writer.assert_called_once()
    written_df, options = writer.call_args[0]
    assert written_df.count() == 1
    assert "writeMethod" not in options
    assert options["table"] == "taxi_analytics.gold_test_table"
    assert options["datePartition"] == "20240101"


def test_partition_export_path_is_table_and_month_scoped():
    path = _partition_export_path(
        {"table": "taxi_analytics.revenue_by_zone_hour", "datePartition": "20240101"},
        {"gcp": {"bucket": "taxi-data-engineer-taxi-lake"}},
    )

    assert path == (
        "gs://taxi-data-engineer-taxi-lake/"
        "gold_export/revenue_by_zone_hour/pickup_month=20240101"
    )


def test_partition_export_path_requires_partition():
    try:
        _partition_export_path(
            {"table": "taxi_analytics.revenue_by_zone_hour"},
            {"gcp": {"bucket": "taxi-data-engineer-taxi-lake"}},
        )
    except ValueError as error:
        assert "datePartition is required" in str(error)
    else:
        raise AssertionError("missing datePartition must fail")
