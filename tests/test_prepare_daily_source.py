from types import SimpleNamespace

import pytest

from scripts.prepare_daily_source import prepare_source


def test_prepare_source_validates_override_and_writes_artifacts(tmp_path):
    source_uri = "gs://bucket/prod/raw/yellow/source_month=2024-01-01/snapshot=" + "a" * 64 + "/file.parquet"

    def runner(args, capture_output, text):
        return SimpleNamespace(returncode=0, stdout=source_uri + "\n")

    result = prepare_source(
        "bucket",
        str(tmp_path / "source.json"),
        source_uri_override="2024-01-01=" + source_uri,
        project="project",
        runner=runner,
    )

    assert result["status"] == "LANDED_OVERRIDE"
    assert (tmp_path / "source-month").read_text() == "2024-01-01"
    assert (tmp_path / "source-uri").read_text() == "2024-01-01=" + source_uri


def test_prepare_source_rejects_missing_override_object(tmp_path):
    def runner(args, capture_output, text):
        return SimpleNamespace(returncode=1, stdout="")

    with pytest.raises(RuntimeError, match="unavailable"):
        prepare_source(
            "bucket",
            str(tmp_path / "source.json"),
            source_uri_override="2024-01-01=gs://bucket/missing",
            project="project",
            runner=runner,
        )
