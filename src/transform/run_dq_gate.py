"""Data quality gate: quarantine rate check.

compute_quarantine_rate  — pure; returns float 0..1
check_quarantine_rate    — raises RuntimeError if rate exceeds threshold
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def compute_quarantine_rate(valid_df: DataFrame, quar_df: DataFrame) -> float:
    """Return fraction of rows that were quarantined (0.0 if total is zero)."""
    valid_count = valid_df.count()
    quar_count = quar_df.count()
    total = valid_count + quar_count
    if total == 0:
        return 0.0
    return quar_count / total


def check_quarantine_rate(
    valid_df: DataFrame,
    quar_df: DataFrame,
    *,
    threshold: float = 0.02,
) -> float:
    """Compute rate and raise RuntimeError if it exceeds threshold.

    Returns the rate so callers can log it without computing it twice.
    """
    rate = compute_quarantine_rate(valid_df, quar_df)
    if rate > threshold:
        raise RuntimeError(
            f"Quarantine rate {rate:.2%} exceeds threshold {threshold:.2%} — aborting Silver write."
        )
    return rate
