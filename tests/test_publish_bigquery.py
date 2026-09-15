import json
from pathlib import Path

import pytest

from scripts.publish_bigquery import load_handoff, publication_sql


def handoff():
    return {
        "pipeline_run_id": "run-1",
        "environment": "staging",
        "project_id": "project-1",
        "dataset": "dataset_staging",
        "months": ["2024-01-01"],
        "gold_metrics": [
            {"mart": "revenue_by_zone_hour", "month": "2024-01-01", "rows": 1, "trips": 2},
            {"mart": "fare_integrity_daily", "month": "2024-01-01", "rows": 1, "trips": 2},
            {"mart": "payment_mix_monthly", "month": "2024-01-01", "rows": 1, "trips": 2},
        ],
        "status": "READY_FOR_SERVING",
    }


def test_load_handoff_requires_publishable_status(tmp_path):
    path = tmp_path / "handoff.json"
    path.write_text(json.dumps(handoff()), encoding="utf-8")
    assert load_handoff(str(path))["pipeline_run_id"] == "run-1"

    invalid = handoff()
    invalid["status"] = "FAILED"
    path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="not publishable"):
        load_handoff(str(path))


def test_publisher_sql_is_transactional_and_partition_scoped():
    sql = publication_sql(handoff(), {
        "revenue_by_zone_hour": "_stage_revenue_by_zone_hour_run-1",
        "fare_integrity_daily": "_stage_fare_integrity_daily_run-1",
        "payment_mix_monthly": "_stage_payment_mix_monthly_run-1",
    })
    assert sql.startswith("BEGIN TRANSACTION;")
    assert sql.endswith("COMMIT TRANSACTION;")
    assert "DELETE FROM" not in sql
    assert sql.count("MERGE") == 4
    assert "pickup_month" in sql
    assert "T.pickup_month = S.pickup_month" in sql
    assert "@run_id" in sql
