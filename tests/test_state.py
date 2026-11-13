"""Tests for delta merging, event ordering and backoff."""

from custom_components.lunatone.state import Backoff, EventBuffer, deep_merge, event_counter


def test_deep_merge_partial_feature_delta() -> None:
    """A devices delta only carries changed fields; the rest is kept."""
    base = {
        "id": 3,
        "name": "Living room",
        "features": {
            "switchable": {"status": False},
            "dimmable": {"status": 0.0},
            "colorRGB": {"status": {"r": 1.0, "g": 1.0, "b": 1.0}},
        },
        "groups": [4],
    }
    delta = {"id": 3, "features": {"switchable": {"status": True}, "dimmable": {"status": 100.0}}}
    merged = deep_merge(base, delta)
    assert merged["features"]["switchable"] == {"status": True}
    assert merged["features"]["dimmable"] == {"status": 100.0}
    assert merged["features"]["colorRGB"] == {"status": {"r": 1.0, "g": 1.0, "b": 1.0}}
    assert merged["name"] == "Living room"
    # The input is not modified.
    assert base["features"]["switchable"] == {"status": False}


def test_deep_merge_replaces_lists_and_nested_colour() -> None:
    """Lists (groups, scenes) are replaced; colour channels merge."""
    base = {"groups": [0], "features": {"colorWAF": {"status": {"w": 0.0, "a": 0.0, "f": 0.0}}}}
    merged = deep_merge(
        base, {"groups": [0, 1], "features": {"colorWAF": {"status": {"w": 0.6}}}}
    )
    assert merged["groups"] == [0, 1]
    assert merged["features"]["colorWAF"]["status"] == {"w": 0.6, "a": 0.0, "f": 0.0}


def _event(counter: int | None, device_id: int = 1) -> dict:
    message: dict = {"type": "devices", "data": {"devices": [{"id": device_id}]}}
    if counter is not None:
        message["timeSignature"] = {"timestamp": 0.0, "counter": counter}
    return message


def test_buffer_discards_events_older_than_snapshot() -> None:
    """Events with counter <= snapshot counter are already in the snapshot."""
    buffer = EventBuffer()
    for counter in (5810, 5813, 5818, 5822):
        buffer.add(_event(counter))
    replayed = [event_counter(m) for m in buffer.drain_after(5814)]
    assert replayed == [5818, 5822]
    assert buffer.events == []


def test_buffer_keeps_arrival_order() -> None:
    """Replay order is arrival order, not sorted."""
    buffer = EventBuffer()
    buffer.add(_event(20, device_id=1))
    buffer.add(_event(15, device_id=2))
    buffer.add(_event(30, device_id=3))
    replayed = [m["data"]["devices"][0]["id"] for m in buffer.drain_after(10)]
    assert replayed == [1, 2, 3]


def test_buffer_without_counters() -> None:
    """Without a snapshot counter (or event counter) events are replayed."""
    buffer = EventBuffer()
    buffer.add(_event(None))
    buffer.add(_event(3))
    assert len(list(buffer.drain_after(None))) == 2
    buffer.add(_event(None))
    assert len(list(buffer.drain_after(100))) == 1


def test_backoff() -> None:
    """Delays double up to the maximum and reset after a success."""
    backoff = Backoff(initial=1, maximum=8)
    assert [backoff.next_delay() for _ in range(6)] == [1, 2, 4, 8, 8, 8]
    backoff.reset()
    assert backoff.next_delay() == 1
