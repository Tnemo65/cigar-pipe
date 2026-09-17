from datetime import date
from types import SimpleNamespace

from scripts.daily_source_landing import candidate_months, discover_latest, download_and_land


def test_candidate_months_returns_newest_first():
    assert candidate_months(date(2024, 3, 15), 2) == [
        "2024-03-01", "2024-02-01", "2024-01-01"
    ]


def test_discover_latest_stops_at_newest_available_file():
    class Session:
        def head(self, url, **kwargs):
            status = 200 if url.endswith("2024-02.parquet") else 404
            return SimpleNamespace(status_code=status, raise_for_status=lambda: None)

    assert discover_latest(date(2024, 3, 15), Session(), 2) == (
        "2024-02-01",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    )


def test_discover_latest_returns_no_data_when_candidates_unavailable():
    class Session:
        def head(self, url, **kwargs):
            return SimpleNamespace(status_code=404, raise_for_status=lambda: None)

    assert discover_latest(date(2024, 3, 15), Session(), 2) is None


def test_download_and_land_is_idempotent():
    class Session:
        def get(self, url, **kwargs):
            return SimpleNamespace(
                status_code=200,
                iter_content=lambda _: [b"a", b"b"],
                raise_for_status=lambda: None,
            )

    result = download_and_land(
        "2024-02-01",
        "https://source/file",
        "project-taxi-lake-prod",
        session=Session(),
        uploader=lambda content, uri: SimpleNamespace(
            already_landed=True, generation="1"
        ),
    )

    assert result["status"] == "ALREADY_LANDED"
    assert result["bytes_received"] == 2
    assert result["uri"].startswith("gs://project-taxi-lake-prod/prod/raw/")
