"""Robust JSON helpers used by GitHub Actions Databricks promotion."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any


def load_json(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def collection(payload: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get(key), list):
        return payload[key]
    raise ValueError(f"Expected a JSON list or object containing '{key}'")


def select_job(payload: Any, name: str, creator: str) -> str:
    jobs = collection(payload, "jobs")
    matches = [
        job
        for job in jobs
        if job.get("settings", {}).get("name") == name
        and job.get("creator_user_name") == creator
        and job.get("settings", {}).get("deployment", {}).get("kind") == "BUNDLE"
    ]
    if not matches:
        raise ValueError(f"No Bundle-managed job found for name={name!r}, creator={creator!r}")
    # Repeated deploys can leave more than one valid Bundle job. Select the newest
    # deployment instead of failing on an unrelated older duplicate.
    selected = max(matches, key=lambda job: int(job.get("created_time", 0)))
    return str(selected["job_id"])


def launch_run(job_id: str, params: dict[str, str], profile: str = "") -> str:
    args = ["databricks", "jobs", "run-now", "--json", json.dumps({"job_id": int(job_id), "job_parameters": params})]
    if profile:
        args.extend(["--profile", profile])
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(f"Databricks run-now failed: {result.stderr.strip()}")
    payload = json.loads(result.stdout)
    run_id = payload.get("run_id") or payload.get("job_run_id")
    if run_id is None:
        raise ValueError(f"run-now response has no run_id: {payload}")
    return str(run_id)


def wait_for_run(run_id: str, timeout_seconds: int = 1800, profile: str = "") -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        args = ["databricks", "jobs", "get-run", run_id, "--output", "json"]
        if profile:
            args.extend(["--profile", profile])
        result = subprocess.run(args, capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(f"Databricks get-run failed: {result.stderr.strip()}")
        payload = json.loads(result.stdout)
        state = payload.get("state", {}).get("life_cycle_state")
        if state in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            if payload.get("state", {}).get("result_state") != "SUCCESS":
                raise RuntimeError(f"Databricks run {run_id} failed: {payload.get('state')}")
            return payload
        time.sleep(15)
    raise TimeoutError(f"Databricks run {run_id} did not finish within {timeout_seconds}s")


def latest_run_id(payload: Any) -> str:
    runs = collection(payload, "runs")
    if not runs:
        raise ValueError("Databricks returned no runs")
    selected = max(runs, key=lambda run: int(run.get("start_time", 0)))
    run_id = selected.get("run_id") or selected.get("job_run_id")
    if run_id is None:
        raise ValueError("Databricks run has no run_id")
    return str(run_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    select_parser = subparsers.add_parser("select-job")
    select_parser.add_argument("--input", required=True)
    select_parser.add_argument("--name", required=True)
    select_parser.add_argument("--creator", required=True)

    run_parser = subparsers.add_parser("latest-run")
    run_parser.add_argument("--input", required=True)

    args = parser.parse_args()
    payload = load_json(args.input)
    if args.command == "select-job":
        print(select_job(payload, args.name, args.creator))
    else:
        print(latest_run_id(payload))


if __name__ == "__main__":
    main()
