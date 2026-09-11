from datetime import datetime
from decimal import Decimal

from src.ingestion.source_identity import (
    SourceObject,
    canonical_payload,
    object_checksum,
    source_event_id,
)


def test_canonical_payload_is_stable_for_mapping_order_and_types():
    first = {"amount": Decimal("1.20"), "at": datetime(2024, 1, 1), "nested": {"b": 2, "a": 1}}
    second = {"nested": {"a": 1, "b": 2}, "at": datetime(2024, 1, 1), "amount": Decimal("1.20")}

    assert canonical_payload(first) == canonical_payload(second)
    assert source_event_id("nyc_tlc", "2024-01-v1", first) == source_event_id(
        "nyc_tlc", "2024-01-v1", second
    )


def test_event_identity_changes_when_snapshot_or_payload_changes():
    payload = {"trip_id": "a", "fare_amount": 10.0}

    assert source_event_id("nyc_tlc", "v1", payload) != source_event_id("nyc_tlc", "v2", payload)
    assert source_event_id("nyc_tlc", "v1", payload) != source_event_id(
        "nyc_tlc", "v1", {**payload, "fare_amount": 11.0}
    )


def test_source_object_key_includes_checksum():
    first = SourceObject("nyc_tlc", "yellow", "v1", "page-1", object_checksum(b"one"))
    second = SourceObject("nyc_tlc", "yellow", "v1", "page-1", object_checksum(b"two"))

    assert first.object_key != second.object_key
