from scripts.publish_bigquery import publish_handoff
from types import SimpleNamespace
from unittest.mock import MagicMock


def test_multi_month_handoff_appends_each_month_to_one_stage_table():
    handoff = {
        "pipeline_run_id": "run-1", "environment": "staging", "project_id": "p", "dataset": "d",
        "months": ["2024-01-01", "2024-02-01"], "snapshots": [], "status": "READY_FOR_SERVING",
        "gold_metrics": [
            {"mart": m, "month": month, "rows": 1, "trips": 1}
            for m in ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly")
            for month in ("2024-01-01", "2024-02-01")
        ],
        "exports": [
            {"mart": m, "month": month, "uri": f"gs://b/{m}/{month}"}
            for m in ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly")
            for month in ("2024-01-01", "2024-02-01")
        ],
    }
    client = MagicMock()
    client.load_table_from_uri.return_value = MagicMock()
    client.get_table.return_value = SimpleNamespace(expires=None)
    client.query.return_value = MagicMock(job_id="job")
    publish_handoff(handoff, client)
    assert client.load_table_from_uri.call_count == 6
    dispositions = [call.kwargs["job_config"].write_disposition for call in client.load_table_from_uri.call_args_list]
    assert dispositions[0] == "WRITE_TRUNCATE"
    assert dispositions[1] == "WRITE_APPEND"
    assert dispositions[2] == "WRITE_TRUNCATE"
    assert dispositions[3] == "WRITE_APPEND"
