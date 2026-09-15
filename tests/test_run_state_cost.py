from unittest.mock import MagicMock, patch

from src.common.run_state import execute_task


def test_execute_task_records_cost_context_on_success():
    state = MagicMock()
    config = {"environment": "staging", "start_month": ""}
    with patch("src.common.run_state.RunState", return_value=state):
        result = execute_task(
            MagicMock(),
            config,
            "gold",
            None,
            lambda _: {"status": "SUCCESS", "snapshots": [{"source_month": "2024-01-01"}]},
        )

    assert result["cost_context"]["environment"] == "staging"
    assert result["cost_context"]["task"] == "gold"
    assert result["cost_context"]["duration_ms"] >= 0
    assert result["cost_context"]["backfill"] is False


def test_execute_task_records_cost_context_for_no_data():
    state = MagicMock()
    state.read.return_value = {"status": "NO_DATA", "snapshots": []}
    with patch("src.common.run_state.RunState", return_value=state):
        result = execute_task(MagicMock(), {"environment": "dev", "start_month": ""}, "gold", "silver", MagicMock())

    assert result["status"] == "NO_DATA"
    assert result["cost_context"]["task"] == "gold"
