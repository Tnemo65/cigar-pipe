# tests/test_run_log.py
from datetime import datetime

from src.common.run_log import log_run


def test_log_run_appends_a_row(spark, tmp_delta_path):
    loc = tmp_delta_path.replace("\\", "/")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS local_run_log (task_name STRING, rows_in BIGINT, "
        f"rows_out BIGINT, rows_quarantined BIGINT, status STRING, "
        f"started_at TIMESTAMP, ended_at TIMESTAMP) USING DELTA LOCATION '{loc}'"
    )
    try:
        log_run(
            spark,
            task_name="transform_silver",
            rows_in=100,
            rows_out=98,
            rows_quarantined=2,
            status="SUCCESS",
            started_at=datetime(2024, 1, 1, 0, 0, 0),
            ended_at=datetime(2024, 1, 1, 0, 5, 0),
            table_name="local_run_log",
        )
        rows = spark.table("local_run_log").collect()
        assert len(rows) == 1
        assert rows[0].status == "SUCCESS"
        assert rows[0].rows_quarantined == 2
    finally:
        spark.sql("DROP TABLE IF EXISTS local_run_log")
