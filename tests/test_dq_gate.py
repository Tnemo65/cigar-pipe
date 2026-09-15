import pytest

from src.transform.run_dq_gate import check_snapshot_metrics


def test_snapshot_metrics_calculates_current_snapshot_rate():
    snapshots = [{"snapshot_id": "source-1", "rows_received": 100}]
    metrics = [{
        "source_snapshot_id": "source-1",
        "rows_in": 100,
        "rows_out": 97,
        "rows_quarantined": 3,
        "raw_rows_quarantined": 3,
        "rows_deduplicated": 0,
    }]

    check_snapshot_metrics(metrics, snapshots, 0.03)
    with pytest.raises(RuntimeError, match="quarantine rate"):
        check_snapshot_metrics(metrics, snapshots, 0.02)


def test_snapshot_metrics_rejects_missing_snapshot_metric():
    with pytest.raises(RuntimeError, match="Missing"):
        check_snapshot_metrics([], [{"snapshot_id": "source-1", "rows_received": 1}], 0.1)
