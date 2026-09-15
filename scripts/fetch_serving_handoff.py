"""Fetch a serving handoff from the staging Delta run-state table."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.databricks_sql import execute_statement
from src.common import paths


def fetch_handoff(profile: str, warehouse_id: str, config: dict, run_id: str) -> dict:
    state_table = paths.catalog_table("reference", "pipeline_run_state", config)

    def run_cli(payload):
        from scripts.cloud_integration import cli

        profile_args = ["--profile", profile] if profile else []
        if "statement_id" in payload:
            return cli(
                "api",
                "get",
                f"/api/2.0/sql/statements/{payload['statement_id']}",
                *profile_args,
            )
        body = Path("artifacts/_handoff_statement.json")
        body.parent.mkdir(parents=True, exist_ok=True)
        body.write_text(json.dumps(payload), encoding="utf-8")
        return cli(
            "api",
            "post",
            "/api/2.0/sql/statements",
            *profile_args,
            "--json",
            f"@{body}",
        )

    response = execute_statement(
        f"SELECT payload FROM {state_table} "
        f"WHERE pipeline_run_id = '{run_id}' AND task_name = 'export_bigquery'",
        warehouse_id,
        run_cli=run_cli,
    )
    rows = response.get("result", {}).get("data_array", [])
    if len(rows) != 1:
        raise RuntimeError(f"Expected one export state row for {run_id}, got {len(rows)}")
    payload = json.loads(rows[0][0])
    handoff = payload.get("serving_handoff")
    if not handoff:
        raise RuntimeError("Export state has no serving_handoff")
    return {**handoff, "status": handoff.get("status", payload.get("serving_status"))}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", default="artifacts/serving-handoff.json")
    args = parser.parse_args()
    config = {
        "environment": "staging",
        "pipeline_run_id": args.run_id,
        "databricks": {"catalog": "taxi_lakehouse_staging"},
        "gcp": {"bucket": "taxi-data-engineer-taxi-lake-staging"},
        "bigquery": {"dataset": "taxi_analytics_staging"},
    }
    handoff = fetch_handoff(args.profile, args.warehouse_id, config, args.run_id)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(handoff, indent=2), encoding="utf-8")
    print(json.dumps(handoff, indent=2))


if __name__ == "__main__":
    main()
