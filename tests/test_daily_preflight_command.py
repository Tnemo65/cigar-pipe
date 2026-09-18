from types import SimpleNamespace

from scripts.prepare_daily_source import verify_gcs_object


def test_preflight_uses_nested_path_safe_gcloud_ls():
    calls = []

    def runner(args, capture_output, text):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout="gs://bucket/prod/raw/file.parquet\n")

    verify_gcs_object("gs://bucket/prod/raw/file.parquet", "project", runner)
    assert calls[0][1:3] == ["storage", "ls"]
