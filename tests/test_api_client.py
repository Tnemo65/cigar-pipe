from src.ingestion.api_client import ingest_snapshot, replay_invariant
from src.ingestion.api_simulator import ApiScenario, SimulatedApiClient


def test_ingest_snapshot_is_replay_safe_with_duplicate_pages_and_timeout():
    events = [{"event_id": f"event-{index}", "value": index} for index in range(5)]
    scenario = ApiScenario(
        duplicate_page_numbers=frozenset({1}),
        timeout_once_page_numbers=frozenset({2}),
    )

    first_pages = []
    first = ingest_snapshot(
        SimulatedApiClient("snapshot-1", events, page_size=2, scenario=scenario),
        on_page=first_pages.append,
    )
    replay = ingest_snapshot(
        SimulatedApiClient("snapshot-1", events, page_size=2, scenario=scenario)
    )

    assert [event["event_id"] for event in first] == [
        "event-0",
        "event-1",
        "event-2",
        "event-3",
        "event-4",
    ]
    assert len(first_pages) == 3
    assert replay_invariant(first, replay)
