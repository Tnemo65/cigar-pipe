"""Export Gold Delta tables to BigQuery via the Spark BigQuery connector.

export_month(table, month, config, writer)
  — reads the Gold Delta partition for the given month and writes to BQ.

Writer is injectable so tests can stub the BQ write without a real connector.
"""
from __future__ import annotations

from typing import Protocol

from pyspark.sql import DataFrame, SparkSession


class BigQueryWriter(Protocol):
    def write(self, df: DataFrame, bq_table: str, month: str) -> None: ...


class _DefaultBigQueryWriter:
    """Production writer using the Spark BigQuery connector."""

    def write(self, df: DataFrame, bq_table: str, month: str) -> None:
        (
            df.write.format("bigquery")
            .option("table", bq_table)
            .option("partitionField", "pickup_month")
            .option("partitionType", "DAY")
            .option("datePartition", month.replace("-", ""))  # YYYYMM → connector expects YYYYMMDD but month-level is YYYYMM01
            .option("writeMethod", "indirect")
            .mode("overwrite")
            .save()
        )


def export_month(
    spark: SparkSession,
    delta_table: str,
    bq_dataset: str,
    bq_table_name: str,
    month: str,
    *,
    writer: BigQueryWriter | None = None,
) -> int:
    """Read one pickup_month partition from a Gold Delta table and write to BigQuery.

    Args:
        spark:         Active SparkSession.
        delta_table:   Unity Catalog 3-part name (e.g. 'taxi_lakehouse.gold.revenue_by_zone_hour').
        bq_dataset:    BigQuery dataset (e.g. 'taxi_analytics').
        bq_table_name: BQ table name without dataset prefix.
        month:         Partition label 'YYYY-MM'.
        writer:        Optional injectable writer (defaults to BQ connector).

    Returns:
        Row count exported.
    """
    df = (
        spark.table(delta_table)
        .filter(f"pickup_month = '{month}'")
    )
    count = df.count()
    if count == 0:
        return 0

    bq_table = f"{bq_dataset}.{bq_table_name}"
    w = writer or _DefaultBigQueryWriter()
    w.write(df, bq_table, month)
    return count
