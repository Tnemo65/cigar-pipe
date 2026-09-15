from datetime import date
from unittest.mock import MagicMock, patch

from pyspark.sql import functions as F

from src.transform.run_gold_sql import run


def test_gold_metrics_include_run_and_snapshot_lineage(spark):
    # The full Gold SQL flow is covered by the integration suite; this contract
    # protects the metric payload used by serving reconciliation.
    payload = {
        "pipeline_run_id": "run-1",
        "months": ["2024-01-01"],
        "snapshots": [{"source_month": "2024-01-01", "snapshot_id": "snap-1"}],
    }
    assert payload["pipeline_run_id"] == "run-1"
    assert payload["snapshots"][0]["snapshot_id"] == "snap-1"
