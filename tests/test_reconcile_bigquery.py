import pytest

from scripts.reconcile_bigquery import MARTS


def test_reconciliation_covers_all_gold_marts():
    assert MARTS == (
        "revenue_by_zone_hour",
        "fare_integrity_daily",
        "payment_mix_monthly",
    )
