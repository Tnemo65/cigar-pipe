import pytest

from src.common import paths


CONFIG = {
    "environment": "dev",
    "databricks": {"catalog": "taxi_lakehouse_dev"},
    "gcp": {"bucket": "demo-proj-taxi-lake-dev"},
}


def test_catalog_table_requires_explicit_runtime_configuration():
    with pytest.raises(ValueError, match="Runtime configuration"):
        paths.catalog_table("silver", "trips_clean")


def test_catalog_table_uses_environment_scoped_catalog():
    assert paths.catalog_table("silver", "trips_clean", CONFIG) == (
        "taxi_lakehouse_dev.silver.trips_clean"
    )


def test_raw_paths_require_environment_and_use_namespaced_bucket_paths():
    assert paths.raw_yellow_path(CONFIG) == "gs://demo-proj-taxi-lake-dev/dev/raw/yellow/"
    assert paths.raw_ref_path(CONFIG) == "gs://demo-proj-taxi-lake-dev/dev/raw/ref/"


def test_checkpoint_and_schema_location_are_distinct_and_environment_namespaced():
    checkpoint = paths.checkpoint_path("bronze", CONFIG)
    schema_location = paths.schema_location_path("bronze", CONFIG)
    assert checkpoint == "gs://demo-proj-taxi-lake-dev/dev/_checkpoints/bronze/"
    assert schema_location == "gs://demo-proj-taxi-lake-dev/dev/_schemas/bronze/"
    assert checkpoint != schema_location


def test_paths_reject_missing_or_mismatched_environment_suffix():
    with pytest.raises(ValueError, match="explicit"):
        paths.raw_yellow_path({"gcp": {"bucket": "demo-proj-taxi-lake-dev"}})
    with pytest.raises(ValueError, match="Bucket must end"):
        paths.raw_yellow_path({**CONFIG, "gcp": {"bucket": "demo-proj-taxi-lake-staging"}})


def test_load_config_reads_yaml(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("gcp:\n  bucket: test-bucket\n  region: us-central1\n")
    result = paths.load_config(str(config_file))
    assert result["gcp"]["bucket"] == "test-bucket"
