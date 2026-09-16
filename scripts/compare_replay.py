"""Compare initial and replay run evidence without publishing replay output."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


REQUIRED_TASKS = {
    "source_landing",
    "reference_preflight",
    "ingest_bronze",
    "transform_silver",
    "dq_gate",
    "aggregate_gold",
    "export_bigquery",
    "monitor",
}


def normalize_run(evidence: dict) -> dict:
    """Accept the run_cd_job wrapper or a raw Databricks run response."""
    run = evidence.get("run") if isinstance(evidence.get("run"), dict) else evidence
    state = run.get("state", {})
    result_state = evidence.get("status") or state.get("result_state")
    tasks = run.get("tasks", evidence.get("tasks", []))
    return {
        "run_id": str(evidence.get("run_id", run.get("run_id", ""))),
        "result_state": result_state,
        "tasks": {task["task_key"]: task for task in tasks},
        "duration": evidence.get("run_duration", run.get("run_duration")),
    }


def compare(initial: dict, replay: dict, initial_handoff: dict | None = None, replay_handoff: dict | None = None) -> dict:
    first = normalize_run(initial)
    second = normalize_run(replay)
    if not first["run_id"] or not second["run_id"]:
        raise ValueError("Initial and replay evidence must contain run IDs")
    if first["run_id"] == second["run_id"]:
        raise ValueError("Initial and replay run IDs must be different")
    if first["result_state"] != "SUCCESS" or second["result_state"] != "SUCCESS":
        raise ValueError(
            "Initial/replay must both succeed: "
            f"{first['result_state']}/{second['result_state']}"
        )
    if set(first["tasks"]) != REQUIRED_TASKS or set(second["tasks"]) != REQUIRED_TASKS:
        raise ValueError("Initial/replay task graph is incomplete")
    failed = {
        task_key: (first["tasks"][task_key].get("state", {}).get("result_state"),
                   second["tasks"][task_key].get("state", {}).get("result_state"))
        for task_key in REQUIRED_TASKS
        if first["tasks"][task_key].get("state", {}).get("result_state") != "SUCCESS"
        or second["tasks"][task_key].get("state", {}).get("result_state") != "SUCCESS"
    }
    if failed:
        raise ValueError(f"Initial/replay task failure: {failed}")
    if initial_handoff is not None and replay_handoff is not None:
        initial_metrics = sorted(initial_handoff.get("gold_metrics", []), key=lambda item: (item.get("mart"), item.get("month")))
        replay_metrics = sorted(replay_handoff.get("gold_metrics", []), key=lambda item: (item.get("mart"), item.get("month")))
        if initial_metrics != replay_metrics:
            raise ValueError("Replay Gold metrics differ from initial metrics")
        if initial_handoff.get("snapshots") != replay_handoff.get("snapshots"):
            raise ValueError("Replay source snapshots differ from initial snapshots")
    return {
        "status": "REPLAY_VERIFIED",
        "initial_run_id": first["run_id"],
        "replay_run_id": second["run_id"],
        "initial_duration_ms": first["duration"],
        "replay_duration_ms": second["duration"],
        "task_keys_match": set(first["tasks"]) == set(second["tasks"]),
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
