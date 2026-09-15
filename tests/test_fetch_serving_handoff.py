import json
from unittest.mock import patch

from scripts.fetch_serving_handoff import fetch_handoff


def test_handoff_contract_is_ready_for_external_publisher():
    payload = {
        "serving_handoff": {
            "pipeline_run_id": "run-1",
            "environment": "staging",
            "project_id": "project-1",
            "dataset": "dataset_staging",
            "months": ["2024-01-01"],
            "snapshots": [],
            "exports": [{"mart": "revenue_by_zone_hour", "month": "2024-01-01", "uri": "gs://x"}],
            "gold_metrics": [{"mart": "revenue_by_zone_hour", "month": "2024-01-01", "rows": 1, "trips": 1}],
            "status": "READY_FOR_SERVING",
        }
    }

    class FakeRunner:
        def __call__(self, payload):
            return {"result": {"data_array": [[json.dumps(payload)]]}, "status": {"state": "SUCCEEDED"}}

    config = {"databricks": {"catalog": "taxi_lakehouse_staging"}, "environment": "staging"}
    with patch("scripts.fetch_serving_handoff.execute_statement", return_value={"result": {"data_array": [[json.dumps(payload)] ]}}):
        result = fetch_handoff("profile", "warehouse", config, "run-1")
    assert result["status"] == "READY_FOR_SERVING"
    assert result["pipeline_run_id"] == "run-1"
