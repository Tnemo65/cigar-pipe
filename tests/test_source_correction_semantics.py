from src.ingestion.source_landing import months_between


def test_source_landing_retry_semantics_helpers_are_month_bounded():
    assert list(months_between("2024-01-01", "2024-01-01")) == ["2024-01-01"]
