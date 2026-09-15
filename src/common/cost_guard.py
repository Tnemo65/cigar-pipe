"""Cost guards for opt-in cloud execution.

Cloud jobs and benchmarks spend real money. Every opt-in cloud runner must
declare its cost envelope and must never target production.
"""
from __future__ import annotations

PRODUCTION_ENVIRONMENTS = frozenset({"prod"})

DEFAULT_MAX_RUNS = 6
DEFAULT_MAX_DURATION_MINUTES = 180


class CostGuardError(RuntimeError):
    """Raised when a cloud run would exceed its declared cost envelope."""


def assert_non_production(environment: str) -> None:
    if environment in PRODUCTION_ENVIRONMENTS:
        raise CostGuardError(
            "Opt-in cloud runs are not allowed against production; use staging or dev"
        )


def assert_confirmed(confirmed: bool) -> None:
    if not confirmed:
        raise CostGuardError(
            "This command spends cloud money. Re-run with --confirm-cost after reviewing the plan"
        )


def assert_run_budget(run_count: int, max_runs: int = DEFAULT_MAX_RUNS) -> None:
    if max_runs <= 0:
        raise CostGuardError("max_runs must be positive")
    if run_count > max_runs:
        raise CostGuardError(
            f"Requested {run_count} cloud runs exceeds the allowed maximum of {max_runs}"
        )


def assert_duration_budget(minutes: int, max_minutes: int = DEFAULT_MAX_DURATION_MINUTES) -> None:
    if max_minutes <= 0:
        raise CostGuardError("max_duration_minutes must be positive")
    if minutes > max_minutes:
        raise CostGuardError(
            f"Requested timeout {minutes}m exceeds the allowed maximum of {max_minutes}m"
        )


def describe_plan(runs: int, timeout_minutes: int, environment: str) -> str:
    return (
        f"Cost plan: environment={environment}, cloud_runs={runs}, "
        f"per_run_timeout_minutes={timeout_minutes}"
    )
