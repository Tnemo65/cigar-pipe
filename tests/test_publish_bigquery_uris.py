from types import SimpleNamespace

from scripts.publish_bigquery import parquet_uris


def test_parquet_uris_returns_exact_objects_without_wildcard():
    class Client:
        def list_blobs(self, bucket, prefix):
            return [
                SimpleNamespace(name=prefix + "_SUCCESS"),
                SimpleNamespace(name=prefix + "part-000.parquet"),
            ]

    result = parquet_uris({"uri": "gs://bucket/prod/gold_export/run/mart/month"}, Client())
    assert result == ["gs://bucket/prod/gold_export/run/mart/month/part-000.parquet"]
    assert "*" not in result[0]
