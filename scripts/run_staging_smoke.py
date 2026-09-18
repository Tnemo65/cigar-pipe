"""Run and verify one bounded staging smoke test.

This is intentionally separate from GitHub expression logic so the same
fail-closed contract can be exercised locally and in CD.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.cloud_integration import run_case
from src.common.cost_guard import assert_confirmed, assert_non_production


def source_exists(source_uri: str, runner=subprocess.run) -> bool:
    gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud.cmd"
    check = runner(
        [gcloud, "storage", "ls", source_uri, "--project=taxi-data-engineer"],
        capture_output=True,
        text=True,
    )
    return check.returncode == 0 and bool(check.stdout.strip())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--job-id", required=True, type=int)
    parser.add_argument("--environment", choices=("dev", "staging"), required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--confirm-cost", action="store_true")
    parser.add_argument("--max-duration-minutes", type=int, default=30)
    parser.add_argument("--source-uri", required=True, metavar="MONTH=GS_URI")
    parser.add_argument("--output", default="artifacts/staging-smoke.json")
    args = parser.parse_args()

    assert_non_production(args.environment)
    assert_confirmed(args.confirm_cost)
    month, separator, source_uri = args.source_uri.partition("=")
    if not separator or not source_uri.startswith("gs://"):
        raise ValueError("source-uri must be MONTH=gs://... for cloud smoke")
    if not source_exists(source_uri):
        raise RuntimeError(f"Source object is not available before cloud run: {source_uri}")
    result = run_case(
        args.profile,
        args.job_id,
        args.start_month,
        args.end_month,
        timeout_seconds=args.max_duration_minutes * 60,
        source_uri=args.source_uri,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
