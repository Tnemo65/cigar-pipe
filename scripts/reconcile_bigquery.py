"""Read-only BigQuery reconciliation for a published staging handoff."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.cloud import bigquery


MARTS = (
    "revenue_by_zone_hour",
    "fare_integrity_daily",
    "payment_mix_monthly",
)


def reconcile(handoff: dict, client=None) -> dict:
    client = client or bigquery.Client(project=handoff["project_id"])
    project = handoff["project_id"]
    dataset = handoff["dataset"]
    month = handoff["months"][0]
    rows = {}
    for mart in MARTS:
        query = f"""
            SELECT COUNT(*) AS row_count, SUM(trip_count) AS trip_count
            FROM `{project}.{dataset}.{mart}`
            WHERE pickup_month = @month
        """
        job = client.query(
            query,
            location="us-central1",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("month", "DATE", month)]
            ),
        )
        row = list(job.result())[0]
        expected = next(metric for metric in handoff["gold_metrics"] if metric["mart"] == mart)
        rows[mart] = {
            "row_count": row.row_count,
            "trip_count": row.trip_count,
            "expected_row_count": expected["rows"],
            "expected_trip_count": expected["trips"],
            "row_count_match": row.row_count == expected["rows"],
            "trip_count_match": row.trip_count == expected["trips"],
        }

    receipt_query = f"""
        SELECT pipeline_run_id, environment, months_json, snapshot_ids_json, published_at
        FROM `{project}.{dataset}.publication_runs`
        WHERE pipeline_run_id = @run_id
    """
    receipt = list(client.query(
        receipt_query,
        location="us-central1",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", handoff["pipeline_run_id"])
            ]
        ),
    ).result())
    result = {"status": "RECONCILED", "pipeline_run_id": handoff["pipeline_run_id"], "marts": rows, "receipt_count": len(receipt)}
    if len(receipt) != 1 or not all(item["row_count_match"] and item["trip_count_match"] for item in rows.values()):
        raise RuntimeError(json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", required=True)
    parser.add_argument("--output", default="artifacts/bigquery-reconciliation.json")
    args = parser.parse_args()
    result = reconcile(json.loads(Path(args.handoff).read_text(encoding="utf-8")))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
