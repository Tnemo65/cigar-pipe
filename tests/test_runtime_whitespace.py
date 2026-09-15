import pytest

from src.common.runtime import runtime_config


def test_runtime_config_trims_bundle_parameter_whitespace():
    config = runtime_config([
        "--environment", "\tstaging",
        "--catalog", "\ttaxi_lakehouse_staging",
        "--bucket", " taxi-data-engineer-taxi-lake-staging ",
        "--project", " taxi-data-engineer ",
        "--dataset", "taxi_analytics_staging ",
        "--pipeline-run-id", " run-1 ",
    ])
    assert config["environment"] == "staging"
    assert config["databricks"]["catalog"] == "taxi_lakehouse_staging"
    assert config["gcp"]["bucket"] == "taxi-data-engineer-taxi-lake-staging"
