"""Small Databricks SQL Statement Execution API client used by ops scripts."""
from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Callable


class DatabricksSqlError(RuntimeError):
    """Raised when a Databricks SQL statement fails or cannot complete."""


def _run_cli(args: list[str]) -> dict[str, Any]:
    """Run a Databricks CLI API command and decode its JSON response."""
    process = subprocess.run(args, capture_output=True, text=True, check=False)
    if process.returncode != 0:
        raise DatabricksSqlError(
            f"Databricks CLI failed with exit code {process.returncode}: "
            f"{process.stderr.strip()}"
        )
    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise DatabricksSqlError(
            f"Databricks CLI returned invalid JSON: {process.stdout[:500]}"
        ) from error


def _profile_args(profile: str | None) -> list[str]:
    return ["--profile", profile] if profile else []


def _submit_statement(payload: dict[str, Any], profile: str | None = None) -> dict[str, Any]:
    return _run_cli(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--json",
            json.dumps(payload),
            *_profile_args(profile),
        ]
    )


def _get_statement(statement_id: str, profile: str | None = None) -> dict[str, Any]:
    return _run_cli(
        [
            "databricks",
            "api",
            "get",
            f"/api/2.0/sql/statements/{statement_id}",
            *_profile_args(profile),
        ]
    )


def execute_statement(
    statement: str,
    warehouse_id: str,
    *,
    timeout_seconds: int = 300,
    poll_seconds: float = 2.0,
    run_cli: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute a SQL statement and wait for a terminal state.

    The Databricks API may return a statement before execution has completed.
    This helper polls the statement endpoint until it reaches a terminal state,
    so callers never mistake an asynchronous response for an empty result.
    """
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "10s",
    }
    request = run_cli or _submit_statement
    response = request(payload)
    statement_id = response.get("statement_id")
    if not statement_id:
        status = response.get("status", {})
        if status.get("state") in {"SUCCEEDED", "CLOSED"}:
            return response
        raise DatabricksSqlError(f"Databricks response has no statement_id: {response}")

    deadline = time.monotonic() + timeout_seconds
    current = response
    while True:
        state = current.get("status", {}).get("state")
        if state in {"SUCCEEDED", "CLOSED"}:
            return current
        if state in {"FAILED", "CANCELED", "CLOSED_WITH_ERROR"}:
            raise DatabricksSqlError(
                f"Databricks SQL statement {statement_id} ended in {state}: {current}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Databricks SQL statement {statement_id} did not finish within "
                f"{timeout_seconds}s"
            )

        time.sleep(poll_seconds)
        current = (
            _get_statement(statement_id)
            if run_cli is None
            else run_cli({"statement_id": statement_id})
        )
