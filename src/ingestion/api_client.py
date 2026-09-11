"""Replay-safe orchestration for paginated source APIs."""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Protocol

from src.ingestion.api_simulator import SimulatedTimeout
from src.ingestion.source_identity import SourcePage


class PageClient(Protocol):
    def fetch_page(self, cursor: str | None = None) -> SourcePage: ...


class SourceIngestionError(RuntimeError):
    """Raised when a paginated snapshot cannot be completed."""


def ingest_snapshot(
    client: PageClient,
    *,
    max_retries: int = 3,
    on_page: Callable[[SourcePage], None] | None = None,
) -> list[dict[str, Any]]:
    """Fetch a complete snapshot and deduplicate repeated event deliveries.

    The callback is invoked once per unique page delivery. A production callback
    should persist a manifest before acknowledging the page. The returned event
    list is deterministic by first-seen event ID and is safe to replay.
    """
    cursor: str | None = None
    events: dict[str, dict[str, Any]] = {}
    completed_pages: set[tuple[int, str | None]] = set()

    while True:
        attempts = 0
        while True:
            try:
                page = client.fetch_page(cursor)
                break
            except SimulatedTimeout as error:
                attempts += 1
                if attempts > max_retries:
                    raise SourceIngestionError(
                        f"page fetch failed after {max_retries} retries: {error}"
                    ) from error

        page_key = (page.page_number, page.cursor)
        if page_key not in completed_pages:
            if on_page:
                on_page(page)
            completed_pages.add(page_key)

        for event in page.events:
            event_id = event.get("event_id")
            if not event_id:
                raise SourceIngestionError(
                    f"snapshot {page.snapshot_id} contains an event without event_id"
                )
            events.setdefault(str(event_id), dict(event))

        if page.next_cursor is None:
            return list(events.values())
        cursor = page.next_cursor


def replay_invariant(
    first_run: Iterable[dict[str, Any]],
    replay_run: Iterable[dict[str, Any]],
) -> bool:
    """Return whether replay produced the same logical event set."""
    first = {str(event["event_id"]): event for event in first_run}
    replay = {str(event["event_id"]): event for event in replay_run}
    return first == replay
