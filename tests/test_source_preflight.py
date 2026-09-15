from scripts.run_staging_smoke import source_exists


def test_source_exists_uses_runner_without_cloud_process():
    def runner(args, capture_output, text):
        return type("Result", (), {"returncode": 0, "stdout": "gs://bucket/object\n"})()

    assert source_exists("gs://bucket/object", runner) is True


def test_source_exists_rejects_missing_object():
    def runner(args, capture_output, text):
        return type("Result", (), {"returncode": 1, "stdout": ""})()

    assert source_exists("gs://bucket/missing", runner) is False
