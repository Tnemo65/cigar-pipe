"""Deterministic alert fingerprints for deduplicated operational alerts."""
from __future__ import annotations

import hashlib


def alert_fingerprint(environment: str, alert_type: str, scope: str) -> str:
    value = "|".join((environment, alert_type, scope))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def should_alert(
    environment: str,
    alert_type: str,
    scope: str,
    last_fingerprint: str | None,
) -> tuple[bool, str]:
    fingerprint = alert_fingerprint(environment, alert_type, scope)
    return fingerprint != last_fingerprint, fingerprint
