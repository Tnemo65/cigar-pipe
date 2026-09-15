"""Write a deterministic DQ breach report from SQL result rows."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_report(rows: list[dict], threshold: float, run_id: str) -> dict:
    breaches = [row for row in rows if float(row["quarantine_rate"]) > threshold]
    return {
        "run_id": run_id,
        "quarantine_rate_threshold": threshold,
        "status": "BLOCKED_BY_DQ" if breaches else "PASS",
        "breaches": breaches,
        "required_action": (
            "Investigate source/reference data; do not increase the threshold without an approved business rule."
            if breaches
            else None
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--threshold", type=float, default=0.10)
    parser.add_argument("--output", default="artifacts/dq-report.json")
    args = parser.parse_args()
    rows = json.loads(Path(args.input).read_text(encoding="utf-8"))
    report = build_report(rows, args.threshold, args.run_id)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
