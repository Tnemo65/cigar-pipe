from scripts.dq_report import build_report


def test_dq_report_blocks_breached_snapshot_without_threshold_override():
    report = build_report([
        {"source_snapshot_id": "snap-march", "quarantine_rate": 0.1577, "rows": 564000},
        {"source_snapshot_id": "snap-feb", "quarantine_rate": 0.09, "rows": 250000},
    ], 0.10, "run-3-month")

    assert report["status"] == "BLOCKED_BY_DQ"
    assert report["breaches"][0]["source_snapshot_id"] == "snap-march"
    assert "do not increase" in report["required_action"]
