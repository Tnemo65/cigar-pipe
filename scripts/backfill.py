"""Run bounded backfills through the same serialized Job as daily ingestion."""
import argparse
import json
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.cost_guard import assert_confirmed, assert_non_production
from src.ingestion.source_landing import months_between


def backfill_parameters(start, end):
    months = list(months_between(start, end))
    if len(months) > 12:
        raise ValueError("Limit each backfill to 12 months")
    return dict(start_month=start, end_month=end, allow_unpublished="false")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--environment", choices=("dev", "staging", "prod"), required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--confirm-cost", action="store_true", help="Acknowledge that this starts a billable cloud run")
    args = parser.parse_args()
    assert_non_production(args.environment)
    assert_confirmed(args.confirm_cost)
    months = list(months_between(args.start_month, args.end_month))
    if len(months) > 12:
        raise ValueError("Limit each backfill to 12 months")
    params = backfill_parameters(args.start_month, args.end_month)
    subprocess.run(["databricks", "jobs", "run-now", "--profile", args.profile,
                    "--json", json.dumps({"job_id": args.job_id, "job_parameters": params})], check=True)


if __name__ == "__main__":
    main()
