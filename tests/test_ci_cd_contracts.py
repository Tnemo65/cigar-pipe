from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_ci_creates_and_uploads_test_evidence():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "mkdir -p artifacts" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "uv sync --locked" in workflow


def test_cd_uses_run_id_from_bundle_output_and_reconciliation():
    workflow = (ROOT / ".github/workflows/cd.yml").read_text()
    assert "Resolve deployed staging job ID" in workflow
    assert "databricks jobs list --output json" in workflow
    assert "launch_run" in (ROOT / "scripts/cd_databricks.py").read_text()
    assert "wait_for_run" in (ROOT / "scripts/cd_databricks.py").read_text()
    helper = (ROOT / "scripts/cd_databricks.py").read_text()
    assert "metadata_file_path" in helper
    assert "deployment" in helper
    assert "first-run-id.txt" in workflow
    assert "replay-run-id.txt" in workflow
    assert "bundle run -o json" not in workflow
    reconciliation = workflow.split("Reconcile the completed staging run", 1)[1]
    assert '--pipeline-run-id "${{ github.run_id }}"' not in reconciliation
    assert 'DATABRICKS_AUTH_TYPE: github-oidc' in workflow
    assert 'fetch_serving_handoff.py' in workflow
    assert 'publish_bigquery.py' in workflow
    assert 'reconcile_bigquery.py' in workflow
    assert '--run-id "$JOB_RUN_ID"' in workflow
    assert "production:" not in workflow


def test_production_workflow_is_manual_and_paused():
    workflow = (ROOT / ".github/workflows/production.yml").read_text()
    assert "workflow_dispatch:" in workflow
    assert "I_UNDERSTAND_PRODUCTION_DEPLOY" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "BUNDLE_VAR_schedule_pause_status: PAUSED" in workflow
    assert "bundle deploy -t prod" in workflow


def test_workflows_use_pinned_tool_versions():
    for path in (ROOT / ".github/workflows/ci.yml", ROOT / ".github/workflows/cd.yml"):
        content = path.read_text()
        assert "setup-cli@main" not in content
        assert 'version: "0.9.18"' in content
        assert "setup-cli@v1.9.0" in content
