import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.publish_bigquery import publish_handoff, stage_table_name


def test_publish_handoff_loads_every_export_before_transaction():
    handoff = {
        "pipeline_run_id": "run-1",
        "environment": "staging",
        "project_id": "project-1",
        "dataset": "dataset_staging",
        "months": ["2024-01-01"],
        "snapshots": [{"snapshot_id": "snapshot-1"}],
        "gold_metrics": [
            {"mart": "revenue_by_zone_hour", "month": "2024-01-01", "rows": 1, "trips": 2},
            {"mart": "fare_integrity_daily", "month": "2024-01-01", "rows": 1, "trips": 2},
            {"mart": "payment_mix_monthly", "month": "2024-01-01", "rows": 1, "trips": 2},
        ],
        "exports": [
            {"mart": mart, "month": "2024-01-01", "uri": f"gs://bucket/{mart}"}
            for mart in ("revenue_by_zone_hour", "fare_integrity_daily", "payment_mix_monthly")
        ],
    }
    client = MagicMock()
    client.load_table_from_uri.return_value = MagicMock()
    client.get_table.return_value = SimpleNamespace(expires=None)
    client.query.return_value = MagicMock(job_id="bq-job")

    result = publish_handoff(handoff, client)

    assert result["status"] == "PUBLISHED"
    assert client.load_table_from_uri.call_count == 3
    assert client.query.call_count == 1
    assert client.query.call_args.args[0].startswith("BEGIN TRANSACTION;")
    assert all("/*.parquet" in call.args[0] for call in client.load_table_from_uri.call_args_list)
    assert all(table.startswith("_stage_") for table in result["staging_tables"].values())
