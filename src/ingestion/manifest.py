"""Pure ingestion-manifest records and idempotency keys."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class IngestionManifestRecord:
    source_system: str
    dataset_name: str
    snapshot_id: str
    object_name: str
    object_checksum: str
    status: str
    request_id: str | None = None
    page_cursor: str | None = None
    page_number: int | None = None
    object_generation: str | None = None
    rows_received: int | None = None
    pipeline_run_id: str | None = None
    first_seen_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None

    @property
    def idempotency_key(self) -> str:
        return "|".join(
            (
                self.source_system,
                self.dataset_name,
                self.snapshot_id,
                self.object_name,
                self.object_checksum,
            )
        )

    def completed(self) -> "IngestionManifestRecord":
        return IngestionManifestRecord(
            **{**self.__dict__, "status": "LANDED", "completed_at": datetime.utcnow()}
        )
