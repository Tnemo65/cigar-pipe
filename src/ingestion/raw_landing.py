"""Immutable raw-object landing primitives for replay-safe ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.ingestion.source_identity import object_checksum


class LandingConflict(RuntimeError):
    """Raised when an object path already contains different content."""


@dataclass(frozen=True)
class LandingResult:
    uri: str
    checksum: str
    generation: str | None
    already_landed: bool


def land_bytes(
    content: bytes,
    gcs_uri: str,
    uploader: Callable[[bytes, str, str], LandingResult],
) -> LandingResult:
    """Land bytes through an uploader that enforces immutable object semantics."""
    if not content:
        raise ValueError("raw object content must not be empty")
    checksum = object_checksum(content)
    result = uploader(content, gcs_uri, checksum)
    if result.checksum != checksum:
        raise LandingConflict(
            f"uploader checksum mismatch for {gcs_uri}: "
            f"expected {checksum}, got {result.checksum}"
        )
    return result


def gcs_immutable_uploader(content: bytes, gcs_uri: str, checksum: str) -> LandingResult:
    """Upload once to GCS using an object-generation precondition.

    The existing object is accepted only when its recorded SHA-256 metadata
    matches the incoming content. A different payload at the same URI is a
    source conflict and must not be silently overwritten.
    """
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud import storage

    bucket_name, object_name = gcs_uri.removeprefix("gs://").split("/", 1)
    blob = storage.Client().bucket(bucket_name).blob(object_name)
    blob.metadata = {"source_sha256": checksum}
    try:
        blob.upload_from_string(
            content,
            content_type="application/octet-stream",
            if_generation_match=0,
        )
        return LandingResult(gcs_uri, checksum, str(blob.generation), False)
    except PreconditionFailed as error:
        blob.reload()
        existing_checksum = (blob.metadata or {}).get("source_sha256")
        if existing_checksum == checksum:
            return LandingResult(gcs_uri, checksum, str(blob.generation), True)
        raise LandingConflict(
            f"immutable object conflict at {gcs_uri}: existing checksum differs"
        ) from error
