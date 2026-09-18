from types import SimpleNamespace

from scripts.prepare_daily_source import verify_gcs_object


def test_preflight_uses_object_get_not_bucket_list():
    class Blob:
        def reload(self):
            return None

    class Bucket:
        def blob(self, name):
            assert name == "prod/raw/file.parquet"
            return Blob()

    class Client:
        def bucket(self, name):
            assert name == "bucket"
            return Bucket()

    verify_gcs_object("gs://bucket/prod/raw/file.parquet", "project", storage_client=Client())
