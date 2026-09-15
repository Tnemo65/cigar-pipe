from src.common.alerting import alert_fingerprint, should_alert


def test_alert_fingerprint_is_stable_and_scoped():
    first = alert_fingerprint("staging", "SOURCE_FRESHNESS", "2024-01-01")
    assert first == alert_fingerprint("staging", "SOURCE_FRESHNESS", "2024-01-01")
    assert first != alert_fingerprint("prod", "SOURCE_FRESHNESS", "2024-01-01")


def test_should_alert_deduplicates_same_condition():
    should_send, fingerprint = should_alert("dev", "NO_DATA", "2024-01-01", None)
    assert should_send is True
    should_send, same = should_alert("dev", "NO_DATA", "2024-01-01", fingerprint)
    assert should_send is False
    assert same == fingerprint
