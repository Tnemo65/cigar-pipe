from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_daily_source_landing_bootstraps_repository_import_path():
    source = (ROOT / "scripts/daily_source_landing.py").read_text()
    assert "sys.path.insert" in source
    assert "Path(__file__).resolve().parents[1]" in source


def test_daily_workflow_always_has_failure_evidence():
    workflow = (ROOT / ".github/workflows/daily-production.yml").read_text()
    assert "Initialize daily evidence" in workflow
    assert 'artifacts/workflow-status.json' in workflow
    assert "if-no-files-found: warn" in workflow
    assert "Daily production pipeline" in workflow
