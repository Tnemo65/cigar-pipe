# tests/test_export_bigquery.py
from unittest.mock import MagicMock

from src.export.export_bigquery import export_month


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
    assert options["writeMethod"] == "direct"
    assert options["table"] == "taxi_analytics.gold_test_table"
    assert options["datePartition"] == "20240101"
