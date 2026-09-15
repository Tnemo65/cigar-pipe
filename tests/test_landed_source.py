import pytest

from src.ingestion.source_landing import _snapshot_from_landed_gcs


def test_landed_gcs_source_registers_without_spark_scan_or_reupload():
    spark = None
    source_uri = (
        "gs://taxi-data-engineer-taxi-lake-staging/staging/raw/"
        "source_month=2024-01-01/snapshot=" + "a" * 64 + "/yellow_tripdata_2024-01.parquet"
    )

    result = _snapshot_from_landed_gcs("2024-01-01", source_uri, {})

    assert result["rows_received"] is None
    assert result["snapshot_id"] == "a" * 64
    assert result["uri"] == source_uri


def test_landed_gcs_source_requires_checksum_addressed_uri():
    with pytest.raises(RuntimeError, match="SHA-256"):
        _snapshot_from_landed_gcs("2024-01-01", "gs://bucket/object", {})
