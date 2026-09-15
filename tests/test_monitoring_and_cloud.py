import pytest
from scripts.cloud_integration import verify_completed_run
from scripts.monitor_pipeline import check_monitoring

CFG = {"environment":"staging", "processing_date":"2024-04-15", "ingestion":{"publication_lag_months":2,"late_arrival_months":2},
       "monitoring":{"max_no_data_runs":40,"max_source_age_days":100,"min_snapshot_rows":100}}


def test_no_data_still_checks_freshness_and_completeness():
    payload = {"status":"NO_DATA", "snapshots":[]}
    check_monitoring(payload, CFG, {"2024-01-01":"a","2024-02-01":"b"}, 1)
    with pytest.raises(RuntimeError,match="NO_DATA"):
        check_monitoring(payload, CFG, {"2024-02-01":"b"}, 40)
    with pytest.raises(RuntimeError,match="freshness"):
        check_monitoring(payload, CFG, {"2023-01-01":"b"}, 1)
    with pytest.raises(RuntimeError,match="completeness"):
        check_monitoring(payload, CFG, {"2024-02-01":"b"}, 1)


def test_monitor_fails_missing_receipt_and_mismatched_counts():
    payload = {"status":"SUCCESS", "snapshots":[{"source_month":"2024-02-01","rows_received":100}],
               "gold_metrics":[{"rows":10}],"serving_metrics":[{"rows":11}],"publication_job_id":"job-1"}
    with pytest.raises(RuntimeError,match="reconciliation"):
        check_monitoring(payload,CFG,{"2024-01-01":"a"},0)


def test_cloud_smoke_cannot_pass_skipped_or_missing_tasks():
    with pytest.raises(RuntimeError,match="missing"):
        verify_completed_run({"state":{"result_state":"SUCCESS"},"tasks":[]})
    with pytest.raises(RuntimeError,match="did not succeed"):
        verify_completed_run({"state":{"result_state":"FAILED"}})
