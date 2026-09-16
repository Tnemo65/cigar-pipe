import json

import pytest

from scripts.cd_databricks import latest_run_id, select_job


def test_select_job_accepts_list_and_chooses_newest_bundle_job():
    payload = [
        {"job_id": 10, "created_time": 1, "settings": {"name": "taxi-pipeline-staging", "deployment": {"kind": "BUNDLE", "metadata_file_path": "/bundle/staging/state.json"}}},
        {"job_id": 11, "created_time": 2, "settings": {"name": "taxi-pipeline-staging", "deployment": {"kind": "BUNDLE", "metadata_file_path": "/bundle/staging/state.json"}}},
    ]
    assert select_job(payload, "taxi-pipeline-staging", "staging") == "11"


def test_select_job_accepts_wrapped_jobs_payload():
    payload = {"jobs": [{"job_id": 12, "created_time": 1, "settings": {"name": "taxi-pipeline-staging", "deployment": {"kind": "BUNDLE", "metadata_file_path": "/bundle/staging/state.json"}}}]}
    assert select_job(payload, "taxi-pipeline-staging", "staging") == "12"


def test_latest_run_accepts_list_and_wrapped_payload():
    assert latest_run_id({"runs": [{"run_id": 1, "start_time": 10}, {"run_id": 2, "start_time": 20}]}) == "2"
    assert latest_run_id([{"job_run_id": 3, "start_time": 1}]) == "3"


def test_selection_rejects_missing_bundle_job():
    with pytest.raises(ValueError, match="No Bundle-managed job"):
        select_job([], "taxi-pipeline-staging", "staging")
