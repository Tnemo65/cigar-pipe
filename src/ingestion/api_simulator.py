"""Deterministic paginated API simulator for replay and failure testing."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.ingestion.source_identity import SourcePage


class SimulatedApiError(RuntimeError):
    """Base exception for deterministic simulated API failures."""


class SimulatedTimeout(SimulatedApiError):
    """Raised for a configured transient page timeout."""


@dataclass(frozen=True)
class ApiScenario:
    duplicate_page_numbers: frozenset[int] = frozenset()
    timeout_once_page_numbers: frozenset[int] = frozenset()
    out_of_order: bool = False


class SimulatedApiClient:
    """Serve event pages while injecting retry and delivery anomalies."""

    def __init__(
        self,
        snapshot_id: str,
        events: list[Mapping[str, Any]],
        page_size: int = 2,
        scenario: ApiScenario | None = None,
    ) -> None:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        self.snapshot_id = snapshot_id
        self.events = list(events)
        self.page_size = page_size
        self.scenario = scenario or ApiScenario()
        self._timeouts_seen: set[int] = set()
        self._last_page: SourcePage | None = None

    def fetch_page(self, cursor: str | None = None) -> SourcePage:
        page_number = 1 if cursor is None else int(cursor)
        if page_number in self.scenario.timeout_once_page_numbers and page_number not in self._timeouts_seen:
            self._timeouts_seen.add(page_number)
            raise SimulatedTimeout(f"simulated timeout on page {page_number}")

        if page_number < 1:
            raise SimulatedApiError(f"invalid page cursor: {cursor}")
        start = (page_number - 1) * self.page_size
        page_events = self.events[start : start + self.page_size]
        if not page_events:
            raise SimulatedApiError(f"page {page_number} is beyond the snapshot")

        if self.scenario.out_of_order and page_number % 2 == 0:
            page_events = list(reversed(page_events))

        next_cursor = str(page_number + 1) if start + self.page_size < len(self.events) else None
        page = SourcePage(
            snapshot_id=self.snapshot_id,
            page_number=page_number,
            cursor=cursor,
            next_cursor=next_cursor,
            events=tuple(page_events),
        )
        self._last_page = page
        if page_number in self.scenario.duplicate_page_numbers:
            return page
        return page

    def iter_pages(self):
        """Yield pages, retrying only configured transient timeouts."""
        cursor = None
        while True:
            try:
                page = self.fetch_page(cursor)
            except SimulatedTimeout:
                page = self.fetch_page(cursor)
            yield page
            if page.page_number in self.scenario.duplicate_page_numbers:
                yield page
            if page.next_cursor is None:
                return
            cursor = page.next_cursor


def collect_unique_events(pages: list[SourcePage]) -> list[Mapping[str, Any]]:
    """Deduplicate simulated deliveries by their stable event_id."""
    unique: dict[str, Mapping[str, Any]] = {}
    for page in pages:
        for event in page.events:
            event_id = event.get("event_id")
            if not event_id:
                raise ValueError("simulated events require event_id")
            unique.setdefault(str(event_id), event)
    return list(unique.values())
