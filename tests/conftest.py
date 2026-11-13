"""Fixtures for the Lunatone tests."""

import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
import socket
import sys
from typing import Any

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant.const import CONF_URL
from homeassistant.core import HomeAssistant

from custom_components.lunatone import coordinator as coordinator_module
from custom_components.lunatone.const import DOMAIN
from custom_components.lunatone.state import Backoff

from .fake_gateway import UID, FakeGateway


if sys.platform == "win32":
    # Development happens on Windows: asyncio's self-pipe uses an AF_INET
    # socket pair there, which pytest-socket blocks. Build it from the real
    # socket class (captured before pytest-socket patches it).
    _RealSocket = socket.socket

    def _socketpair(
        family: int = socket.AF_INET, type: int = socket.SOCK_STREAM, proto: int = 0
    ) -> tuple[socket.socket, socket.socket]:
        listener = _RealSocket(family, type, proto)
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            first = _RealSocket(family, type, proto)
            first.connect(listener.getsockname())
            fd, _ = listener._accept()  # noqa: SLF001 - accept() would use the guarded class
            second = _RealSocket(family, type, proto, fileno=fd)
        finally:
            listener.close()
        return first, second

    socket.socketpair = _socketpair  # type: ignore[assignment]


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable loading custom_components/lunatone."""


@pytest.fixture(autouse=True)
def fast_timers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make reconnection and dim settling fast."""
    monkeypatch.setattr(coordinator_module, "DIM_SETTLE_DELAY", 0.05)
    monkeypatch.setattr(
        coordinator_module, "Backoff", lambda: Backoff(initial=0.05, maximum=0.2)
    )


@pytest.fixture
async def gateway(socket_enabled: None) -> AsyncGenerator[FakeGateway]:
    """Start a fake gateway on localhost (real sockets, 127.0.0.1 only)."""
    fake = FakeGateway()
    await fake.start()
    yield fake
    await fake.close()


@pytest.fixture
def config_entry(gateway: FakeGateway) -> MockConfigEntry:
    """Return a config entry pointing at the fake gateway."""
    return MockConfigEntry(
        domain=DOMAIN,
        title=gateway.url,
        data={CONF_URL: gateway.url},
        unique_id=UID.replace("-", ""),
    )


async def wait_for(
    condition: Callable[[], bool], timeout: float = 3.0
) -> None:
    """Wait until condition() is true."""
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


@pytest.fixture
def setup_integration(
    hass: HomeAssistant, config_entry: MockConfigEntry
) -> Callable[..., Coroutine[Any, Any, MockConfigEntry]]:
    """Return a coroutine that sets up the integration."""

    async def _setup(options: dict[str, Any] | None = None) -> MockConfigEntry:
        config_entry.add_to_hass(hass)
        if options is not None:
            hass.config_entries.async_update_entry(config_entry, options=options)
        assert await hass.config_entries.async_setup(config_entry.entry_id)
        await hass.async_block_till_done()
        # The one-off read of real levels/colours runs right after the first sync.
        await asyncio.wait_for(config_entry.runtime_data.coordinator.initial_sweep_done.wait(), 5)
        return config_entry

    return _setup
