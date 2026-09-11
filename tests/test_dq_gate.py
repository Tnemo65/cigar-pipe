# tests/test_dq_gate.py
import pytest

from src.transform.run_dq_gate import check_quarantine_rate, compute_quarantine_rate


def _seed(spark, clean_count, quarantine_count, month="2024-01-01"):
    spark.sql("DROP TABLE IF EXISTS local_clean")
    spark.sql("DROP TABLE IF EXISTS local_quarantine")
    clean_rows = [(month,)] * clean_count
    quarantine_rows = [(f"{month} 08:00:00",)] * quarantine_count
    spark.createDataFrame(clean_rows, ["pickup_month"]).createOrReplaceTempView("local_clean")
    spark.createDataFrame(
        quarantine_rows, ["tpep_pickup_datetime"]
    ).createOrReplaceTempView("local_quarantine")


def test_compute_quarantine_rate(spark):
    _seed(spark, clean_count=98, quarantine_count=2)
    rate = compute_quarantine_rate(
        spark, "2024-01-01", clean_table="local_clean", quarantine_table="local_quarantine"
    )
    assert rate == pytest.approx(0.02)


def test_check_quarantine_rate_passes_under_threshold(spark):
    _seed(spark, clean_count=99, quarantine_count=1)
    check_quarantine_rate(
        spark, "2024-01-01", threshold=0.02,
        clean_table="local_clean", quarantine_table="local_quarantine",
    )  # must not raise


def test_check_quarantine_rate_raises_over_threshold(spark):
    _seed(spark, clean_count=90, quarantine_count=10)
    with pytest.raises(RuntimeError, match="quarantine rate"):
        check_quarantine_rate(
            spark, "2024-01-01", threshold=0.02,
            clean_table="local_clean", quarantine_table="local_quarantine",
        )
