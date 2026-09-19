from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_daily_production_uses_explicit_production_job_id():
    workflow = (ROOT / ".github/workflows/daily-production.yml").read_text()
    assert "PRODUCTION_JOB_ID" in workflow
    assert "jobs list" not in workflow
    assert "--job-id" in workflow
