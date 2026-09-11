import pytest

from src.ingestion.api_simulator import (
    ApiScenario,
    SimulatedApiClient,
    SimulatedTimeout,
    collect_unique_events,
)


def _events(count=5):
    return [{"event_id": f"event-{index}", "value": index} for index in range(count)]


def test_simulator_paginates_and_retries_timeout():
    client = SimulatedApiClient(
        "snapshot-1",
        _events(),
        page_size=2,
        scenario=ApiScenario(timeout_once_page_numbers=frozenset({2})),
    )

    pages = list(client.iter_pages())

    assert [page.page_number for page in pages] == [1, 2, 3]
    assert [event["event_id"] for event in pages[1].events] == ["event-2", "event-3"]


def test_simulator_timeout_is_transient_for_direct_fetch():
    client = SimulatedApiClient(
        "snapshot-1",
        _events(),
        scenario=ApiScenario(timeout_once_page_numbers=frozenset({1})),
    )

    with pytest.raises(SimulatedTimeout):
        client.fetch_page()
    assert client.fetch_page().page_number == 1


def test_duplicate_deliveries_are_collapsed_by_event_id():
    pages = list(
        SimulatedApiClient(
            "snapshot-1",
            _events(),
            page_size=2,
            scenario=ApiScenario(duplicate_page_numbers=frozenset({1})),
        ).iter_pages()
    )

    unique = collect_unique_events(pages)

    assert [event["event_id"] for event in unique] == [
        "event-0",
        "event-1",
        "event-2",
        "event-3",
        "event-4",
    ]


def test_simulator_can_return_out_of_order_events_without_losing_pages():
    client = SimulatedApiClient(
        "snapshot-1",
        _events(4),
        page_size=2,
        scenario=ApiScenario(out_of_order=True),
    )

    unique = collect_unique_events(list(client.iter_pages()))

    assert {event["event_id"] for event in unique} == {
        "event-0",
        "event-1",
        "event-2",
        "event-3",
    }
