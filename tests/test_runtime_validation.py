import pytest

from src.common.runtime import runtime_config


def test_runtime_rejects_project_ids_longer_than_gcp_limit():
    with pytest.raises(ValueError, match="Invalid project"):
        runtime_config([
            "--environment", "dev",
            "--catalog", "taxi_lakehouse_dev",
            "--bucket", "taxi-lake-dev",
            "--project", "a" + "b" * 29 + "0",
            "--dataset", "taxi_analytics_dev",
            "--pipeline-run-id", "run-1",
        ])
