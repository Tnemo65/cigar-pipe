import pytest

from src.common.runtime import runtime_config


def base_args(*extra):
    return [
        "--environment", "staging",
        "--catalog", "taxi_lakehouse_staging",
        "--bucket", "taxi-data-engineer-taxi-lake-staging",
        "--project", "taxi-data-engineer",
        "--dataset", "taxi_analytics_staging",
        "--pipeline-run-id", "run-1",
        "--start-month", "2024-01-01",
        "--end-month", "2024-02-01",
        *extra,
    ]


def test_source_uri_map_must_cover_all_requested_months():
    with pytest.raises(ValueError, match="cover every requested month"):
        runtime_config(base_args("--source-uri", "2024-01-01=gs://bucket/jan"))


def test_source_uri_map_covers_requested_months():
    result = runtime_config(base_args(
        "--source-uri",
        "2024-01-01=gs://bucket/jan;2024-02-01=gs://bucket/feb",
    ))
    assert set(result["source_uris"]) == {"2024-01-01", "2024-02-01"}
