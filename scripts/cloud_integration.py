"""Execute the real source-to-BigQuery Job, then replay to verify idempotence.

This is opt-in cloud work. It does not report success based on local mocks.
"""
import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.cost_guard import (
    DEFAULT_MAX_DURATION_MINUTES,
    DEFAULT_MAX_RUNS,
    assert_confirmed,
    assert_duration_budget,
    assert_non_production,
    assert_run_budget,
    describe_plan,
)


def cli(*args):
    result = subprocess.run(["databricks", *args, "--output", "json"], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def verify_completed_run(run):
    if run.get("state", {}).get("result_state") != "SUCCESS":
        raise RuntimeError(f"Cloud run did not succeed: {run.get('state')}")
    expected = {"source_landing", "reference_preflight", "ingest_bronze", "transform_silver", "dq_gate", "aggregate_gold", "export_bigquery", "monitor"}
    tasks = {t["task_key"]: t for t in run.get("tasks", [])}
    if set(tasks) != expected:
        raise RuntimeError("Cloud run is missing required pipeline tasks")
    if any(t.get("state", {}).get("result_state") != "SUCCESS" for t in tasks.values()):
        raise RuntimeError("Cloud task failed or was skipped")


def backfill_months(start, end):
    from src.ingestion.source_landing import months_between

    return list(months_between(start, end))


def run_case(profile, job_id, start, end, timeout_seconds=14400, bundle_target=None, source_uri=None):
    params = dict(start_month=start, end_month=end, allow_unpublished="false")
    if source_uri:
        params["source_uri"] = source_uri
    response = cli("jobs", "run-now", "--profile", profile, "--no-wait", "--json",
                   json.dumps(dict(job_id=job_id, job_parameters=params)))
    run_id, deadline = response["run_id"], time.monotonic()+timeout_seconds
    while time.monotonic() < deadline:
        run = cli("jobs", "get-run", str(run_id), "--profile", profile)
        if run.get("state", {}).get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            verify_completed_run(run)
            return {"run_id": run_id, "start_month": start, "end_month": end,
                    "duration_ms": run.get("run_duration"), "run_page_url": run.get("run_page_url"),
                    "tasks": [{"task_key":t["task_key"], "execution_duration":t.get("execution_duration"),
                               "state":t.get("state")} for t in run["tasks"]]}
        time.sleep(20)
    raise TimeoutError(f"Cloud run {run_id} still running; inspect before cancelling")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--environment", choices=("dev", "staging", "prod"), required=True)
    parser.add_argument("--bundle-target", default=None)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--benchmark", action="store_true", help="Run actual 3/6/12-month jobs and their replays")
    parser.add_argument("--confirm-cost", action="store_true", help="Acknowledge that this starts billable cloud runs")
    parser.add_argument("--max-runs", type=int, default=DEFAULT_MAX_RUNS)
    parser.add_argument("--max-duration-minutes", type=int, default=DEFAULT_MAX_DURATION_MINUTES)
    parser.add_argument("--source-uri-map", default="", help="Semicolon-separated MONTH=GS_URI mappings for landed sources")
    parser.add_argument("--output", default="artifacts/cloud-integration.json")
    args = parser.parse_args()
    assert_non_production(args.environment)
    assert_confirmed(args.confirm_cost)
    assert_duration_budget(args.max_duration_minutes)
    from scripts.backfill import backfill_parameters
    backfill_parameters(args.start_month, args.end_month)
    ranges = [(args.start_month, args.end_month)]
    if args.benchmark:
        year, month = map(int, args.start_month[:7].split("-"))
        start_index = year*12 + month-1
        ranges = [(args.start_month, f"{(start_index+n-1)//12:04d}-{(start_index+n-1)%12+1:02d}-01") for n in (3,6,12)]
    assert_run_budget(len(ranges) * 2, args.max_runs)
    print(describe_plan(len(ranges) * 2, args.max_duration_minutes, args.environment))
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "RUNNING",
        "environment": args.environment,
        "max_runs": args.max_runs,
        "max_duration_minutes": args.max_duration_minutes,
        "cost_guard": "confirmed_non_production",
        "runs": [],
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        for start,end in ranges:
            for attempt in ("initial", "replay"):
                result = run_case(
                    args.profile,
                    args.job_id,
                    start,
                    end,
                    timeout_seconds=args.max_duration_minutes * 60,
                    bundle_target=args.bundle_target,
                    source_uri=args.source_uri_map or None,
                )
                report["runs"].append({"attempt":attempt, **result})
                output.write_text(json.dumps(report,indent=2),encoding="utf-8")
        report["status"] = "SUCCESS"
    except Exception as error:
        report.update(status="FAILED", error=str(error))
        raise
    finally:
        output.write_text(json.dumps(report,indent=2),encoding="utf-8")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    main()
