from src.common import paths


def test_catalog_table_builds_three_level_name():
    assert paths.catalog_table("silver", "trips_clean") == "taxi_lakehouse.silver.trips_clean"


def test_catalog_table_uses_configured_catalog():
    config = {"databricks": {"catalog": "dev_lakehouse"}}
    assert paths.catalog_table("silver", "trips_clean", config) == (
        "dev_lakehouse.silver.trips_clean"
    )


def test_raw_yellow_path_uses_configured_bucket():
    config = {"gcp": {"bucket": "demo-proj-taxi-lake"}}
    assert paths.raw_yellow_path(config) == "gs://demo-proj-taxi-lake/raw/yellow/"


def test_raw_ref_path_uses_configured_bucket():
    config = {"gcp": {"bucket": "demo-proj-taxi-lake"}}
    assert paths.raw_ref_path(config) == "gs://demo-proj-taxi-lake/raw/ref/"


def test_checkpoint_and_schema_location_are_distinct_and_namespaced():
    config = {"gcp": {"bucket": "demo-proj-taxi-lake"}}
    ckpt = paths.checkpoint_path("bronze", config)
    schema_loc = paths.schema_location_path("bronze", config)
    assert ckpt == "gs://demo-proj-taxi-lake/_checkpoints/bronze/"
    assert schema_loc == "gs://demo-proj-taxi-lake/_schemas/bronze/"
    assert ckpt != schema_loc


def test_load_config_reads_yaml(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("gcp:\n  bucket: test-bucket\n  region: us-central1\n")
    result = paths.load_config(str(config_file))
    assert result["gcp"]["bucket"] == "test-bucket"
