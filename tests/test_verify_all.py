import pytest

from scripts.verify_all import assert_quarantine_rate


def test_assert_quarantine_rate_accepts_rate_at_threshold():
    assert assert_quarantine_rate(100, 10, 0.10) == pytest.approx(0.10)


def test_assert_quarantine_rate_rejects_rate_above_threshold():
    with pytest.raises(AssertionError, match="exceeds threshold"):
        assert_quarantine_rate(100, 11, 0.10)


def test_assert_quarantine_rate_rejects_empty_input():
    with pytest.raises(ValueError, match="positive"):
        assert_quarantine_rate(0, 0, 0.10)
