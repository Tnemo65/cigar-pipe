"""Launch and verify a single Databricks job run without guessing its ID."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cd_databricks import launch_run, select_job, wait_for_run


def load_job_id(path: str, name: str, target: str) -> str:
    return select_job(json.loads(Path(path).read_text(encoding="utf-8")), name, target)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs-json")
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--job-id", default="")
    parser.add_argument("--target", required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--profile", default="")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.job_id:
        job_id = args.job_id
    elif args.jobs_json:
        job_id = load_job_id(args.jobs_json, args.job_name, args.target)
    else:
        raise ValueError("Provide either --job-id or --jobs-json")
    run_id = launch_run(
        job_id,
        {
            "start_month": args.start_month,
            "end_month": args.end_month,
            "source_uri": args.source_uri,
            "allow_unpublished": "false",
        },
        args.profile,
    )
    run = wait_for_run(run_id, args.timeout_seconds, args.profile)
    output = {
        "job_id": job_id,
        "run_id": run_id,
        "status": run.get("state", {}).get("result_state"),
        "run": run,
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(run_id)


if __name__ == "__main__":
    main()
