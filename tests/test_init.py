"""Integration tests against the fake gateway."""

import asyncio

from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant.components.cover import CoverState
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import ATTR_ENTITY_ID, STATE_OFF, STATE_ON, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, entity_registry as er

from custom_components.lunatone.const import (
    CONF_DEFAULT_COLORS,
    CONF_DEVICE_TYPES,
    CONF_GROUP_TYPES,
    CONF_INVERTED_COVERS,
    DOMAIN,
)

from .conftest import wait_for
from .fake_gateway import UID, FakeGateway, make_device

UNIQUE = UID.replace("-", "")


def _on(level: float) -> dict:
    return {"switchable": {"status": True}, "dimmable": {"status": level}}


def _entity_id(hass: HomeAssistant, domain: str, unique_id: str) -> str:
    entity_id = er.async_get(hass).async_get_entity_id(domain, DOMAIN, unique_id)
    assert entity_id, f"{domain} {unique_id} not registered"
    return entity_id


async def test_setup_creates_entities_by_type(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """DT6/DT8/DT0 become lights, DT7 a switch, group 4 a group light."""
    entry = await setup_integration()
    assert entry.state is ConfigEntryState.LOADED

    light = _entity_id(hass, "light", f"{UNIQUE}-device1")
    rgbw = _entity_id(hass, "light", f"{UNIQUE}-device2")
    _entity_id(hass, "switch", f"{UNIQUE}-device3")
    _entity_id(hass, "light", f"{UNIQUE}-device4")
    _entity_id(hass, "light", f"{UNIQUE}-line0-group4")
    _entity_id(hass, "light", f"{UNIQUE}-line0")

    assert hass.states.get(light).state == STATE_OFF
    assert hass.states.get(rgbw).attributes["supported_color_modes"] == ["rgbw"]


async def test_state_only_changes_from_websocket(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """A command is sent over REST; the state changes only when the event arrives."""
    await setup_integration()
    light = _entity_id(hass, "light", f"{UNIQUE}-device1")

    await hass.services.async_call("light", "turn_on", {ATTR_ENTITY_ID: light}, blocking=True)
    assert gateway.frames[-1] == [0x00, 254]  # DAPC 254 to short address 0
    assert hass.states.get(light).state == STATE_OFF

    await gateway.send_delta(
        1, {"features": {"switchable": {"status": True}, "dimmable": {"status": 100.0}}}
    )
    await wait_for(lambda: hass.states.get(light).state == STATE_ON)
    assert hass.states.get(light).attributes["brightness"] == 255


async def test_rgbw_single_command(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """RGB and WAF are loaded first, then one DAPC applies colour and level together."""
    await setup_integration()
    rgbw = _entity_id(hass, "light", f"{UNIQUE}-device2")
    gateway.frames.clear()

    await hass.services.async_call(
        "light",
        "turn_on",
        {ATTR_ENTITY_ID: rgbw, "rgbw_color": (255, 0, 0, 51), "brightness": 255},
        blocking=True,
    )
    assert gateway.frames == [
        [0xA3, 254], [0xC3, 0], [0xC5, 0], [0xC1, 8], [0x03, 0xEB],  # RGB to A1
        [0xA3, 51], [0xC3, 0], [0xC5, 0], [0xC1, 8], [0x03, 0xEC],  # WAF to A1
        [0x02, 254],  # one DAPC applies both
    ]
    assert not gateway.requests  # nothing over REST


async def test_group_light_aggregates_members(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """Group state comes from the members; commands go to the group."""
    await setup_integration()
    group = _entity_id(hass, "light", f"{UNIQUE}-line0-group4")
    assert hass.states.get(group).state == STATE_OFF

    await gateway.send(
        "devices",
        {
            "devices": [
                {"id": 1, "features": _on(100.0)},
                {"id": 2, "features": _on(50.0)},
            ]
        },
    )
    await wait_for(lambda: hass.states.get(group).state == STATE_ON)
    # mean of 100 % and 50 %
    assert hass.states.get(group).attributes["brightness"] == 191

    gateway.frames.clear()
    await hass.services.async_call("light", "turn_off", {ATTR_ENTITY_ID: group}, blocking=True)
    assert gateway.frames == [[0x88, 0]]  # DAPC 0 to group 4


async def test_snapshot_race_uses_global_counter(
    hass: HomeAssistant, gateway: FakeGateway, config_entry: MockConfigEntry
) -> None:
    """Events buffered during GET /devices are replayed only if newer than the snapshot."""
    gateway.devices_gate = asyncio.Event()
    config_entry.add_to_hass(hass)
    setup = hass.async_create_task(hass.config_entries.async_setup(config_entry.entry_id))

    await asyncio.wait_for(gateway.devices_requested.wait(), 3)
    snapshot_counter = gateway.counter  # GET /devices consumed this counter
    # Already in the snapshot: the gateway changed state before the request.
    # (sent with an older counter while the response is in flight)
    await gateway.send(
        "devices",
        {"devices": [{"id": 1, "features": _on(3.0)}]},
        counter=snapshot_counter - 1,
    )
    # Newer than the snapshot: must be applied on top of it.
    await gateway.send_delta(
        4, {"features": {"switchable": {"status": True}, "dimmable": {"status": 40.0}}}
    )
    gateway.devices_gate.set()
    assert await setup
    await hass.async_block_till_done()

    coordinator = config_entry.runtime_data.coordinator
    # Device 1 comes from the snapshot (off), the older event was dropped.
    assert coordinator.data[1].is_on is False
    # Device 4's newer event was replayed.
    assert coordinator.data[4].brightness == 40.0


async def test_reconnect_marks_unavailable_and_resyncs(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """Entities are unavailable while disconnected; a new snapshot is applied on reconnect."""
    await setup_integration()
    light = _entity_id(hass, "light", f"{UNIQUE}-device1")

    gateway.devices_gate = asyncio.Event()  # keep the next sync pending
    gateway.devices_requested.clear()
    await gateway.drop_clients()
    await wait_for(lambda: hass.states.get(light).state == STATE_UNAVAILABLE)

    # Changed while we were away: the next snapshot must contain it.
    gateway.devices[1]["features"]["switchable"]["status"] = True
    gateway.devices[1]["features"]["dimmable"]["status"] = 70.0
    await asyncio.wait_for(gateway.devices_requested.wait(), 3)
    gateway.devices_gate.set()
    await wait_for(lambda: hass.states.get(light).state == STATE_ON)


async def test_new_and_deleted_devices(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """devices events add entities; devicesDeleted removes them and the registry device."""
    await setup_integration()

    new = make_device(9, 8, [6])
    gateway.devices[9] = new
    await gateway.send("devices", {"devices": [new]})
    await wait_for(
        lambda: er.async_get(hass).async_get_entity_id("light", DOMAIN, f"{UNIQUE}-device9")
        is not None
    )

    await gateway.send("devicesDeleted", {"deleted": [9]})
    await wait_for(
        lambda: er.async_get(hass).async_get_entity_id("light", DOMAIN, f"{UNIQUE}-device9") is None
    )
    assert dr.async_get(hass).async_get_device(identifiers={(DOMAIN, f"{UNIQUE}-device9")}) is None


async def test_cover_motion_from_dali_monitor(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """A device typed as cover follows UP/OFF/DOWN seen on the bus and sends UP/DOWN/OFF."""
    await setup_integration({CONF_DEVICE_TYPES: {"4": "cover"}})
    cover = _entity_id(hass, "cover", f"{UNIQUE}-device4")
    assert er.async_get(hass).async_get_entity_id("light", DOMAIN, f"{UNIQUE}-device4") is None

    address_byte = (3 << 1) | 1
    await gateway.send_monitor([address_byte, 0x01])  # UP from a wall switch
    await wait_for(lambda: hass.states.get(cover).state == CoverState.OPENING)
    await gateway.send_monitor([address_byte, 0x00])  # OFF
    await wait_for(lambda: hass.states.get(cover).state != CoverState.OPENING)
    await gateway.send_monitor([address_byte, 0x06])  # RECALL MIN
    await wait_for(lambda: hass.states.get(cover).state == CoverState.CLOSING)

    gateway.frames.clear()
    await hass.services.async_call("cover", "open_cover", {ATTR_ENTITY_ID: cover}, blocking=True)
    await hass.services.async_call("cover", "stop_cover", {ATTR_ENTITY_ID: cover}, blocking=True)
    await hass.services.async_call("cover", "close_cover", {ATTR_ENTITY_ID: cover}, blocking=True)
    assert gateway.frames == [[0x07, 0x01], [0x07, 0x00], [0x07, 0x02]]  # UP, OFF, DOWN


async def test_dim_burst_queries_actual_level(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """UP frames from a switch are not tracked by the gateway: query the real level."""
    await setup_integration()
    light = _entity_id(hass, "light", f"{UNIQUE}-device1")
    gateway.levels = {0: 127, 1: 254}
    gateway.received.clear()  # ignore the initial sweep

    for _ in range(3):
        await gateway.send_monitor([0x89, 0x01])  # group 4: UP
    await wait_for(lambda: hass.states.get(light).state == STATE_ON)

    queries = [m["data"]["daliData"] for m in gateway.received if m["type"] == "daliFrame"]
    assert sorted(queries) == [[1, 0xA0], [3, 0xA0]]  # one query per member, not per frame
    assert hass.states.get(light).attributes["brightness"] == 128


async def test_own_frames_do_not_trigger_queries(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """Frames sent by the gateway itself (externalSource false) are ignored for dimming."""
    await setup_integration()
    gateway.received.clear()  # ignore the initial sweep
    await gateway.send_monitor([0x89, 0x01], external=False)
    await asyncio.sleep(0.2)
    assert not [m for m in gateway.received if m["type"] == "daliFrame"]


async def test_unload(hass: HomeAssistant, gateway: FakeGateway, setup_integration) -> None:
    """Unloading closes the WebSocket."""
    entry = await setup_integration()
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED
    await wait_for(lambda: all(ws.closed for ws in gateway.clients))


async def test_default_color_when_turned_on_without_color(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """The configured normal colour is sent with the level when switching on."""
    await setup_integration({CONF_DEFAULT_COLORS: {"device2": [0, 0, 0, 254]}})
    rgbw = _entity_id(hass, "light", f"{UNIQUE}-device2")
    gateway.frames.clear()

    await hass.services.async_call("light", "turn_on", {ATTR_ENTITY_ID: rgbw}, blocking=True)
    # The normal colour is given in DALI levels and sent unchanged: W = 254.
    assert [0xA3, 254] in gateway.frames
    assert gateway.frames[-1] == [0x02, 254]

    # Already on: a brightness change keeps the current colour.
    await gateway.send_delta(2, {"features": _on(100.0)})
    await wait_for(lambda: hass.states.get(rgbw).state == STATE_ON)
    gateway.frames.clear()
    await hass.services.async_call(
        "light", "turn_on", {ATTR_ENTITY_ID: rgbw, "brightness": 128}, blocking=True
    )
    assert len(gateway.frames) == 1
    assert gateway.frames[0][0] == 0x02 and gateway.frames[0][1] in (127, 128)


async def test_options_flow(hass: HomeAssistant, gateway: FakeGateway, setup_integration) -> None:
    """Types and colours chosen in the options flow are stored and applied after reload."""
    entry = await setup_integration()

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["step_id"] == "init"
    labels = {str(key): key for key in result["data_schema"].schema}
    device4 = next(label for label in labels if label.endswith("id 4"))
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {device4: "cover"}
    )
    assert result["step_id"] == "groups"
    result = await hass.config_entries.options.async_configure(result["flow_id"], {})
    assert result["step_id"] == "colors"
    color_labels = [str(key) for key in result["data_schema"].schema]
    rgbw_label = next(label for label in color_labels if label.endswith("id 2"))

    bad = await hass.config_entries.options.async_configure(
        result["flow_id"], {rgbw_label: "300,0,0"}
    )
    assert bad["errors"] == {rgbw_label: "invalid_color"}
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {rgbw_label: "0,0,0,254"}
    )
    assert result["step_id"] == "covers"
    cover_label = next(str(key) for key in result["data_schema"].schema)
    assert cover_label.endswith("id 4")
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {cover_label: True}
    )
    assert result["type"] == "create_entry"
    await hass.async_block_till_done()

    assert entry.options[CONF_DEVICE_TYPES]["4"] == "cover"
    assert entry.options[CONF_INVERTED_COVERS] == ["4"]
    assert entry.options[CONF_DEFAULT_COLORS] == {"device2": [0, 0, 0, 254]}
    await wait_for(
        lambda: er.async_get(hass).async_get_entity_id("cover", DOMAIN, f"{UNIQUE}-device4")
        is not None
    )
    assert er.async_get(hass).async_get_entity_id("light", DOMAIN, f"{UNIQUE}-device4") is None


async def test_rest_retries_stale_connection(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """A ServerDisconnectedError on a reused REST connection is retried once."""
    from unittest.mock import patch

    import aiohttp

    from custom_components.lunatone.coordinator import Auth

    entry = await setup_integration()
    real_request = Auth.request
    calls = 0

    async def flaky(self, method, path, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise aiohttp.ServerDisconnectedError
        return await real_request(self, method, path, **kwargs)

    with patch.object(Auth, "request", flaky):
        response = await entry.runtime_data.coordinator.auth.get("devices")
    assert response.status == 200
    assert calls == 2


async def test_missing_confirmation_is_not_an_error(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """The real gateway sometimes executes a frame without confirming it."""
    await setup_integration()
    light = _entity_id(hass, "light", f"{UNIQUE}-device1")
    gateway.drop_confirmations = 1
    await hass.services.async_call("light", "turn_on", {ATTR_ENTITY_ID: light}, blocking=True)
    # The next command is matched to its own confirmation.
    gateway.frame_result = 1
    import pytest

    from homeassistant.exceptions import HomeAssistantError

    with pytest.raises(HomeAssistantError, match="result 1"):
        await hass.services.async_call(
            "light", "turn_off", {ATTR_ENTITY_ID: light}, blocking=True
        )


async def test_command_while_disconnected_raises(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """Commands fail clearly when the WebSocket is down."""
    import pytest

    from homeassistant.exceptions import HomeAssistantError

    entry = await setup_integration()
    gateway.devices_gate = asyncio.Event()  # keep the reconnection pending
    await gateway.drop_clients()
    coordinator = entry.runtime_data.coordinator
    await wait_for(lambda: not coordinator.connected)
    with pytest.raises(HomeAssistantError, match="Not connected"):
        await coordinator.async_send_frames(0, [[0x00, 254]])
    gateway.devices_gate.set()


async def test_initial_sweep_reads_real_level_and_colour(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """The gateway reports what it inferred; the drivers report what they really do."""
    # Gateway snapshot: RGBW off, colour 1/1/1 W0. Driver: on at 100 %, RGB 0, W 254.
    gateway.levels = {1: 254}
    gateway.colours = {1: {233: 0, 234: 0, 235: 0, 236: 254}}
    entry = await setup_integration()
    rgbw = _entity_id(hass, "light", f"{UNIQUE}-device2")

    state = hass.states.get(rgbw)
    assert state.state == STATE_ON
    assert state.attributes["rgbw_color"] == (0, 0, 0, 255)

    reads = [f for f in gateway.frames if f[1] in (0xA0, 0xFA)]
    # one level query per light (devices 1, 2, 4) and four colour queries for the RGBW
    assert len(reads) == 3 + 4
    assert entry.runtime_data.coordinator.initial_sweep_done.is_set()


async def test_inverted_cover(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """A module wired the other way round: open sends DOWN and UP on the bus means closing."""
    await setup_integration({CONF_DEVICE_TYPES: {"4": "cover"}, CONF_INVERTED_COVERS: ["4"]})
    cover = _entity_id(hass, "cover", f"{UNIQUE}-device4")

    gateway.frames.clear()
    await hass.services.async_call("cover", "open_cover", {ATTR_ENTITY_ID: cover}, blocking=True)
    await hass.services.async_call("cover", "close_cover", {ATTR_ENTITY_ID: cover}, blocking=True)
    assert gateway.frames == [[0x07, 0x02], [0x07, 0x01]]  # DOWN opens, UP closes

    await gateway.send_monitor([0x07, 0x01])  # UP from a wall switch
    await wait_for(lambda: hass.states.get(cover).state == CoverState.CLOSING)


async def test_group_cover_with_mixed_wiring(
    hass: HomeAssistant, gateway: FakeGateway, setup_integration
) -> None:
    """A group mixing normal and inverted modules is commanded member by member."""
    gateway.devices[4]["groups"] = [6]
    gateway.devices[5] = make_device(5, 4, [0], groups=[6])
    await setup_integration(
        {
            CONF_DEVICE_TYPES: {"4": "cover", "5": "cover"},
            CONF_GROUP_TYPES: {"0-6": "cover"},
            CONF_INVERTED_COVERS: ["4"],
        }
    )
    group = _entity_id(hass, "cover", f"{UNIQUE}-line0-group6")

    gateway.frames.clear()
    await hass.services.async_call("cover", "open_cover", {ATTR_ENTITY_ID: group}, blocking=True)
    # no group frame; A3 (inverted) gets DOWN, A4 gets UP
    assert sorted(gateway.frames) == [[0x07, 0x02], [0x09, 0x01]]

    gateway.frames.clear()
    await hass.services.async_call("cover", "stop_cover", {ATTR_ENTITY_ID: group}, blocking=True)
    assert gateway.frames == [[0x8D, 0x00]]  # stop is the same for everyone: one group OFF
