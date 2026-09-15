import pytest

from src.common.cost_guard import (
    CostGuardError,
    assert_confirmed,
    assert_duration_budget,
    assert_non_production,
    assert_run_budget,
)


def test_cost_guard_requires_explicit_confirmation():
    with pytest.raises(CostGuardError, match="--confirm-cost"):
        assert_confirmed(False)


def test_cost_guard_rejects_production_runs():
    with pytest.raises(CostGuardError, match="production"):
        assert_non_production("prod")
    assert_non_production("staging")


def test_cost_guard_limits_runs_and_duration():
    with pytest.raises(CostGuardError, match="maximum"):
        assert_run_budget(7, 6)
    with pytest.raises(CostGuardError, match="maximum"):
        assert_duration_budget(181, 180)
    assert_run_budget(6, 6)
    assert_duration_budget(180, 180)
