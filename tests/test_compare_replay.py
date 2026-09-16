import pytest

from scripts.compare_replay import compare


def run(run_id):
    tasks = [{"task_key": key} for key in ("source_landing", "reference_preflight", "ingest_bronze", "transform_silver", "dq_gate", "aggregate_gold", "export_bigquery", "monitor")]
    return {"run_id": run_id, "run_duration": 1, "state": {"result_state": "SUCCESS"}, "tasks": tasks}


def test_compare_replay_requires_distinct_successful_runs():
    result = compare(run("initial"), run("replay"))
    assert result["status"] == "REPLAY_VERIFIED"


def test_compare_replay_rejects_same_run():
    with pytest.raises(ValueError, match="different"):
        compare(run("same"), run("same"))


def test_compare_replay_rejects_missing_task():
    broken = run("broken")
    broken["tasks"] = broken["tasks"][:-1]
    with pytest.raises(ValueError, match="incomplete"):
        compare(run("initial"), broken)
