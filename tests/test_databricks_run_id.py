from src.common.databricks_run import extract_run_id


def test_extracts_top_level_run_id():
    assert extract_run_id('{"run_id": 123}') == "123"


def test_extracts_run_id_from_bundle_run_url():
    assert extract_run_id(
        'Run URL: https://workspace/jobs/100/runs/430414159804232?o=1'
    ) == "430414159804232"


def test_extracts_run_id_from_job_run_url():
    assert extract_run_id(
        '{"run_page_url":"https://workspace/?o=1#job/100/run/430414159804232"}'
    ) == "430414159804232"
