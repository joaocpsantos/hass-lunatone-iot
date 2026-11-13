"""Pure helpers for merging gateway state and ordering WebSocket events."""

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any


def deep_merge(base: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    """Return base updated with delta; nested dicts are merged, everything else replaced."""
    merged = dict(base)
    for key, value in delta.items():
        current = merged.get(key)
        if isinstance(value, dict) and isinstance(current, dict):
            merged[key] = deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def event_counter(message: dict[str, Any]) -> int | None:
    """Return the timeSignature counter of a WebSocket message or REST response."""
    signature = message.get("timeSignature")
    if isinstance(signature, dict) and isinstance(signature.get("counter"), int):
        return signature["counter"]
    return None


@dataclass
class EventBuffer:
    """Holds state events received while the REST snapshot is being fetched.

    The gateway uses one global, monotonic counter for REST responses and
    WebSocket events, so after a snapshot with counter N only the buffered
    events with a counter greater than N are newer than the snapshot.
    """

    events: list[dict[str, Any]] = field(default_factory=list)

    def add(self, message: dict[str, Any]) -> None:
        """Buffer a message."""
        self.events.append(message)

    def drain_after(self, snapshot_counter: int | None) -> Iterator[dict[str, Any]]:
        """Yield, in arrival order, the buffered events newer than the snapshot."""
        events, self.events = self.events, []
        for message in events:
            counter = event_counter(message)
            if snapshot_counter is None or counter is None or counter > snapshot_counter:
                yield message


@dataclass
class Backoff:
    """Exponential reconnection delay."""

    initial: float = 1.0
    maximum: float = 60.0
    _next: float = 0.0

    def reset(self) -> None:
        """Reset after a successful connection."""
        self._next = 0.0

    def next_delay(self) -> float:
        """Return the delay before the next attempt."""
        self._next = self.initial if self._next == 0 else min(self._next * 2, self.maximum)
        return self._next
