from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_prepare_daily_source_bootstraps_repository_import_path():
    source = (ROOT / "scripts/prepare_daily_source.py").read_text()
    assert "sys.path.insert" in source
    assert "Path(__file__).resolve().parents[1]" in source
