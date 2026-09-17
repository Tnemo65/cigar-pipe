from src.common.run_state import validate_state


def test_no_data_replay_with_zero_metrics_is_valid():
    validate_state("NO_DATA", {
        "reason": "snapshot_already_processed",
        "snapshots": [{"snapshot_id": "snap"}],
        "metrics": [{"rows_in": 0}],
    })
