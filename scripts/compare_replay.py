"""Compare initial and replay run evidence without publishing replay output."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def compare(initial: dict, replay: dict) -> dict:
    if initial["run_id"] == replay["run_id"]:
        raise ValueError("Initial and replay run IDs must be different")
    initial_state = initial.get("state", {}).get("result_state")
    replay_state = replay.get("state", {}).get("result_state")
    if initial_state != "SUCCESS" or replay_state != "SUCCESS":
        raise ValueError(f"Initial/replay must both succeed: {initial_state}/{replay_state}")
    initial_tasks = {task["task_key"]: task for task in initial.get("tasks", [])}
    replay_tasks = {task["task_key"]: task for task in replay.get("tasks", [])}
    required = {"source_landing", "reference_preflight", "ingest_bronze", "transform_silver", "dq_gate", "aggregate_gold", "export_bigquery", "monitor"}
    if set(initial_tasks) != required or set(replay_tasks) != required:
        raise ValueError("Initial/replay task graph is incomplete")
    return {
        "status": "REPLAY_VERIFIED",
        "initial_run_id": initial["run_id"],
        "replay_run_id": replay["run_id"],
        "initial_duration_ms": initial.get("run_duration"),
        "replay_duration_ms": replay.get("run_duration"),
        "task_keys_match": set(initial_tasks) == set(replay_tasks),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial", required=True)
    parser.add_argument("--replay", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = compare(load(args.initial), load(args.replay))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
