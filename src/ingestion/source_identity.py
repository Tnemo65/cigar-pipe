"""Stable identities for replay-safe source ingestion."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def canonical_payload(payload: Mapping[str, Any]) -> str:
    """Serialize a payload deterministically for hashing and comparison."""
    return json.dumps(_json_value(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def source_event_id(
    source_system: str,
    snapshot_id: str,
    payload: Mapping[str, Any],
) -> str:
    """Create an immutable identity for one logical source event/version."""
    material = "|".join((source_system, snapshot_id, canonical_payload(payload)))
    return sha256_text(material)


def object_checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@dataclass(frozen=True)
class SourceObject:
    source_system: str
    dataset: str
    snapshot_id: str
    object_name: str
    checksum: str

    @property
    def object_key(self) -> str:
        return "|".join(
            (self.source_system, self.dataset, self.snapshot_id, self.object_name, self.checksum)
        )


@dataclass(frozen=True)
class SourcePage:
    snapshot_id: str
    page_number: int
    cursor: str | None
    next_cursor: str | None
    events: tuple[Mapping[str, Any], ...]
