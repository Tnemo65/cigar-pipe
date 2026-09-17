from unittest.mock import MagicMock, patch

from src.transform.run_dq_gate import run


def test_dq_replay_zero_rows_is_no_data_not_failure():
    state = MagicMock()
    config = {"environment": "prod", "thresholds": {"quarantine_rate_max": 0.1}, "monitoring": {"min_snapshot_rows": 100}}
    payload = {"snapshots": [{"snapshot_id": "snap"}], "metrics": [{"source_snapshot_id": "snap", "rows_in": 0, "rows_out": 0, "rows_quarantined": 0, "rows_deduplicated": 0, "raw_rows_quarantined": 0}]}
    state.read.return_value = payload
    with patch("src.common.run_state.RunState", return_value=state):
        result = run(MagicMock(), config)
    assert result["status"] == "NO_DATA"
