import pytest

from types import SimpleNamespace

from scripts.run_staging_smoke import main


def test_smoke_requires_landed_gcs_source(monkeypatch):
    monkeypatch.setattr("sys.argv", [
        "run_staging_smoke.py", "--profile", "p", "--job-id", "1",
        "--environment", "staging", "--start-month", "2024-01-01",
        "--end-month", "2024-01-01", "--source-uri", "2024-01-01=gs://missing/object",
        "--confirm-cost",
    ])
    monkeypatch.setattr("scripts.run_staging_smoke.source_exists", lambda uri: False)
    with pytest.raises(RuntimeError, match="not available"):
        main()
