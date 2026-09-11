"""Shared runner for Gold SQL files.

run_gold_sql_file(path, month, spark)
  — reads the SQL file, substitutes :month, executes statement by statement.

All three Gold marts use this runner so :month substitution is consistent.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def run_gold_sql_file(path: str, month: str, spark: SparkSession) -> None:
    """Execute a Gold SQL file with :month substituted.

    Args:
        path:  Absolute or relative path to the .sql file.
        month: Partition label in 'YYYY-MM' format (e.g. '2024-01').
        spark: Active SparkSession.
    """
    sql_text = open(path).read()
    sql_text = sql_text.replace(":month", month)
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)
