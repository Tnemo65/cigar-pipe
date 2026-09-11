from unittest.mock import MagicMock, patch

from scripts.download_tlc_data import download_month, download_zone_lookup

CONFIG = {"gcp": {"bucket": "demo-proj-taxi-lake"}}


@patch("scripts.download_tlc_data.requests.get")
def test_download_month_uploads_to_correct_path(mock_get):
    mock_get.return_value = MagicMock(status_code=200, content=b"fake-parquet-bytes")
    uploader = MagicMock()

    result = download_month(2024, 1, CONFIG, uploader=uploader)

    mock_get.assert_called_once_with(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
        timeout=120,
    )
    uploader.assert_called_once_with(
        b"fake-parquet-bytes",
        "gs://demo-proj-taxi-lake/raw/yellow/yellow_tripdata_2024-01.parquet",
    )
    assert result == "gs://demo-proj-taxi-lake/raw/yellow/yellow_tripdata_2024-01.parquet"


@patch("scripts.download_tlc_data.requests.get")
def test_download_month_raises_on_http_error(mock_get):
    mock_get.return_value = MagicMock(status_code=404, content=b"")
    try:
        download_month(2099, 1, CONFIG, uploader=MagicMock())
        assert False, "expected an exception for a 404"
    except RuntimeError as e:
        assert "404" in str(e)


@patch("scripts.download_tlc_data.requests.get")
def test_download_zone_lookup(mock_get):
    mock_get.return_value = MagicMock(status_code=200, content=b"LocationID,Borough,Zone,service_zone\n")
    uploader = MagicMock()

    result = download_zone_lookup(CONFIG, uploader=uploader)

    mock_get.assert_called_once_with(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", timeout=30
    )
    assert result == "gs://demo-proj-taxi-lake/raw/ref/taxi_zone_lookup.csv"
