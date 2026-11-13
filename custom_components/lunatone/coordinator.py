"""WebSocket (local push) coordinator for a Lunatone DALI-2 IoT gateway."""

import asyncio
from collections import deque
from dataclasses import dataclass
import logging
from typing import Any, override

import aiohttp
from lunatone_rest_api_client import Auth, Device, Info
from lunatone_rest_api_client.models import DeviceData, InfoData, LineStatus
from pydantic import ValidationError
from yarl import URL

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_URL
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    CONF_DEFAULT_COLORS,
    CONF_DEVICE_TYPES,
    CONF_GROUP_TYPES,
    CONF_INVERTED_COVERS,
    DOMAIN,
    CoverMotion,
    EntityType,
)
from .commands import DT8, DTR0, ENABLE_DEVICE_TYPE
from .dali import (
    LEVEL_UNTRACKED_OPCODES,
    MASK,
    MAX_LEVEL,
    QUERY_ACTUAL_LEVEL,
    QUERY_COLOUR_VALUE,
    ForwardFrame,
    Opcode,
    TargetKind,
    level_to_percent,
    parse_monitor_event,
    short_address_byte,
)
from .state import Backoff, EventBuffer, deep_merge, event_counter

_LOGGER = logging.getLogger(__name__)

HEARTBEAT = 30
INFO_TIMEOUT = 10
FIRST_SYNC_TIMEOUT = 20
QUERY_TIMEOUT = 2
# Time for the gateway to confirm each daliFrame we send.
FRAME_TIMEOUT = 1.5
FRAME_SPACING = 0.025
COMMAND_PRIORITY = 2
QUERY_PRIORITY = 4
# Time without UP/DOWN frames before the real level of the dimmed lights is queried.
DIM_SETTLE_DELAY = 1.0
DALI_TYPE_SWITCHING = 7

# daliStatus codes (DALI-2 IoT API documentation, table 2)
DALI_STATUS_LINE_STATUS = {
    0: LineStatus.NO_POWER,
    2: LineStatus.OK,
    5: LineStatus.LOW_POWER,
}

COVER_MOTION_BY_OPCODE = {
    Opcode.RECALL_MAX_LEVEL: CoverMotion.OPENING,
    Opcode.UP: CoverMotion.OPENING,
    Opcode.RECALL_MIN_LEVEL: CoverMotion.CLOSING,
    Opcode.DOWN: CoverMotion.CLOSING,
    Opcode.OFF: CoverMotion.STOPPED,
}

type GroupKey = tuple[int, int]


@dataclass
class LunatoneData:
    """Data for Lunatone integration."""

    coordinator: LunatoneCoordinator


type LunatoneConfigEntry = ConfigEntry[LunatoneData]


class RetryingAuth(Auth):
    """REST client that retries once when the gateway closed an idle keep-alive connection.

    The gateway drops idle HTTP connections after a few seconds; reusing one
    fails immediately with ServerDisconnectedError (seen on real hardware).
    """

    async def request(self, method: str, path: str, **kwargs: Any) -> aiohttp.ClientResponse:
        """Make a request, retrying once on a stale connection."""
        try:
            return await super().request(method, path, **kwargs)
        except aiohttp.ServerDisconnectedError:
            _LOGGER.debug("Stale connection for %s /%s, retrying", method, path)
            return await super().request(method, path, **kwargs)


class WebSocketInfo(Info):
    """Gateway info fed from the WebSocket greeting instead of GET /info."""

    def set_data(self, data: InfoData) -> None:
        """Replace the info data."""
        self._data = data


def default_device_type(device: DeviceData) -> EntityType:
    """Return the entity type used when the user did not choose one."""
    if DALI_TYPE_SWITCHING in device.dali_types:
        return EntityType.SWITCH
    return EntityType.LIGHT


class LunatoneCoordinator(DataUpdateCoordinator[dict[int, Device]]):
    """Keeps the gateway state in sync over one WebSocket connection.

    State is only written from the WebSocket stream: on every (re)connection
    the socket is opened first, state events are buffered, GET /devices is
    fetched as a snapshot and only buffered events newer than the snapshot
    (global timeSignature counter) are replayed on top of it.
    """

    config_entry: LunatoneConfigEntry

    def __init__(self, hass: HomeAssistant, config_entry: LunatoneConfigEntry) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN}-{config_entry.title}",
            update_interval=None,
        )
        url = URL(config_entry.data[CONF_URL])
        self._session = async_get_clientsession(hass)
        self.auth = RetryingAuth(self._session, config_entry.data[CONF_URL])
        self.info = WebSocketInfo(self.auth)
        self._ws_url = url.with_scheme("wss" if url.scheme == "https" else "ws").with_path("/")

        self.data = {}
        self.connected = False
        self.scan: dict[str, Any] | None = None
        self.device_motion: dict[int, CoverMotion] = {}

        self._raw: dict[int, dict[str, Any]] = {}
        self._buffer: EventBuffer | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._task: asyncio.Task[None] | None = None
        self._synced = asyncio.Event()
        self.initial_sweep_done = asyncio.Event()
        self._info_received = asyncio.Event()
        self._backoff = Backoff()
        self._fetching: set[int] = set()

        # Commands and queries are sent one sequence at a time; the gateway
        # confirms every frame with one daliFrame message, in order.
        self._send_lock = asyncio.Lock()
        self._frame_results: deque[asyncio.Future[int | None]] = deque()
        self._pending_answer: tuple[list[int], asyncio.Future[int | None]] | None = None
        self._dim_pending: set[int] = set()
        self._dim_unsub: CALLBACK_TYPE | None = None

    @override
    async def _async_update_data(self) -> dict[int, Device]:
        """Return the current data; updates are pushed, never polled."""
        return self.data

    # ------------------------------------------------------------- lifecycle

    async def async_start(self) -> bool:
        """Start the WebSocket loop and wait for the first snapshot."""
        self._task = self.config_entry.async_create_background_task(
            self.hass, self._run(), f"{DOMAIN} websocket {self.config_entry.title}"
        )
        try:
            async with asyncio.timeout(FIRST_SYNC_TIMEOUT):
                await self._synced.wait()
        except TimeoutError:
            await self.async_stop()
            return False
        return True

    async def async_stop(self) -> None:
        """Stop the WebSocket loop."""
        if self._dim_unsub:
            self._dim_unsub()
            self._dim_unsub = None
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        """Connect, sync and listen forever, reconnecting with backoff."""
        while True:
            try:
                await self._connect_and_listen()
                error: Exception = ConnectionError("WebSocket closed by the gateway")
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, TimeoutError, OSError, ValidationError) as err:
                error = err
            except Exception as err:  # noqa: BLE001 - keep the loop alive
                _LOGGER.exception("Unexpected error in the Lunatone WebSocket loop")
                error = err
            self._set_disconnected(error)
            delay = self._backoff.next_delay()
            _LOGGER.debug("Reconnecting to %s in %.0f s", self._ws_url, delay)
            await asyncio.sleep(delay)

    async def _connect_and_listen(self) -> None:
        _LOGGER.debug("Connecting to %s", self._ws_url)
        async with self._session.ws_connect(self._ws_url, heartbeat=HEARTBEAT) as ws:
            self._ws = ws
            self._buffer = EventBuffer()
            self._info_received.clear()
            reader = asyncio.create_task(self._read(ws))
            try:
                async with asyncio.timeout(INFO_TIMEOUT):
                    await self._info_received.wait()
                await self._sync_snapshot()
                self._backoff.reset()
                await reader
            finally:
                reader.cancel()
                try:
                    await reader
                except (asyncio.CancelledError, Exception):
                    pass
                self._ws = None
                self._buffer = None

    async def _read(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type is aiohttp.WSMsgType.TEXT:
                try:
                    message = msg.json()
                except ValueError:
                    _LOGGER.debug("Ignoring non-JSON message: %s", msg.data[:200])
                    continue
                if isinstance(message, dict):
                    self._handle_message(message)
            elif msg.type is aiohttp.WSMsgType.ERROR:
                raise ws.exception() or ConnectionError("WebSocket error")
        _LOGGER.debug("WebSocket closed (code %s)", ws.close_code)

    async def _sync_snapshot(self) -> None:
        response = await self.auth.get("devices")
        payload = await response.json()
        counter = event_counter(payload)

        # No awaits from here on: buffered events and the snapshot are
        # combined atomically before live events are processed again.
        previous = set(self.data)
        self._raw = {
            device["id"]: device
            for device in payload.get("devices", [])
            if isinstance(device, dict) and isinstance(device.get("id"), int)
        }
        replayed = 0
        assert self._buffer is not None
        for message in self._buffer.drain_after(counter):
            self._apply_state_event(message)
            replayed += 1
        self._buffer = None
        self.data = {}
        self._rebuild(set(self._raw))
        for device_id in previous - set(self.data):
            self._remove_registry_device(device_id)

        first_sync = not self._synced.is_set()
        if not self.connected and not first_sync:
            _LOGGER.info("Reconnected to %s", self._ws_url)
        self.connected = True
        _LOGGER.debug(
            "Snapshot #%s applied: %d devices, %d buffered events replayed",
            counter,
            len(self.data),
            replayed,
        )
        self._synced.set()
        self.async_set_updated_data(self.data)
        if first_sync:
            self.config_entry.async_create_background_task(
                self.hass, self._async_initial_sweep(), f"{DOMAIN} initial sweep"
            )

    @callback
    def _set_disconnected(self, error: Exception) -> None:
        if self.connected:
            _LOGGER.warning("Lost connection to %s: %s", self._ws_url, error)
        else:
            _LOGGER.debug("Connection to %s failed: %s", self._ws_url, error)
        self.connected = False
        if self._dim_unsub:
            self._dim_unsub()
            self._dim_unsub = None
        self._dim_pending.clear()
        self._fail_pending()
        # Not async_set_update_error: it would log every failed reconnection as an error.
        self.last_exception = error
        if self.last_update_success:
            self.last_update_success = False
        self.async_update_listeners()

    # -------------------------------------------------------------- messages

    @callback
    def _handle_message(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        data = message.get("data")
        if msg_type in ("devices", "devicesDeleted"):
            if self._buffer is not None:
                self._buffer.add(message)
                return
            self._rebuild(self._apply_state_event(message))
            self.async_set_updated_data(self.data)
        elif not isinstance(data, dict):
            return
        elif msg_type == "info":
            self._handle_info(data)
        elif msg_type == "daliMonitor":
            self._handle_monitor(data)
        elif msg_type == "daliAnswer":
            self._handle_answer(data)
        elif msg_type == "daliFrame":
            self._handle_frame_result(data)
        elif msg_type == "daliStatus":
            self._handle_dali_status(data)
        elif msg_type == "scanProgress":
            self.scan = data
            self.async_update_listeners()

    def _apply_state_event(self, message: dict[str, Any]) -> set[int]:
        """Merge a devices/devicesDeleted event into the raw state; return touched ids."""
        data = message.get("data") or {}
        touched: set[int] = set()
        if message.get("type") == "devices":
            for delta in data.get("devices", []):
                if not isinstance(delta, dict) or not isinstance(delta.get("id"), int):
                    continue
                device_id = delta["id"]
                self._raw[device_id] = deep_merge(self._raw.get(device_id, {}), delta)
                touched.add(device_id)
        else:
            for device_id in data.get("deleted", []):
                if self._raw.pop(device_id, None) is not None:
                    touched.add(device_id)
        return touched

    @callback
    def _rebuild(self, device_ids: set[int]) -> None:
        """Rebuild library Device objects from the raw state."""
        for device_id in device_ids:
            raw = self._raw.get(device_id)
            if raw is None:
                if self.data.pop(device_id, None) is not None:
                    self.device_motion.pop(device_id, None)
                    self._remove_registry_device(device_id)
                continue
            try:
                device_data = DeviceData.model_validate(raw)
            except ValidationError:
                # A partial delta for a device we have never seen in full.
                self._fetch_device(device_id)
                continue
            self.data[device_id] = Device(self.info, device_data)

    @callback
    def _fetch_device(self, device_id: int) -> None:
        if device_id in self._fetching:
            return
        self._fetching.add(device_id)
        self.config_entry.async_create_background_task(
            self.hass, self._async_fetch_device(device_id), f"{DOMAIN} fetch device"
        )

    async def _async_fetch_device(self, device_id: int) -> None:
        try:
            response = await self.auth.get(f"device/{device_id}")
            full = await response.json()
        except aiohttp.ClientError as err:
            _LOGGER.debug("Unable to fetch device %s: %s", device_id, err)
            return
        finally:
            self._fetching.discard(device_id)
        if device_id not in self._raw:
            return
        self._raw[device_id] = deep_merge(full, self._raw[device_id])
        self._rebuild({device_id})
        self.async_set_updated_data(self.data)

    @callback
    def _handle_info(self, data: dict[str, Any]) -> None:
        try:
            info = InfoData.model_validate(data)
        except ValidationError as err:
            _LOGGER.error("Unsupported info message from the gateway: %s", err)
            return
        self.info.set_data(info)
        self._info_received.set()
        if self.connected:
            self.async_update_listeners()

    @callback
    def _handle_dali_status(self, data: dict[str, Any]) -> None:
        status = DALI_STATUS_LINE_STATUS.get(data.get("status"))
        line = self.info.data.lines.get(str(data.get("line"))) if self.info.data else None
        if status is None or line is None or line.line_status == status:
            return
        line.line_status = status
        self.async_update_listeners()

    # ---------------------------------------------------------- daliMonitor

    @callback
    def _handle_monitor(self, data: dict[str, Any]) -> None:
        frame = parse_monitor_event(data)
        if frame is None or frame.is_query:
            return
        changed = self._update_motion(frame)
        if frame.external and frame.opcode in LEVEL_UNTRACKED_OPCODES:
            self._schedule_level_query(frame)
        if changed:
            self.async_update_listeners()

    def _targeted(self, frame: ForwardFrame) -> list[int]:
        return [
            device_id
            for device_id, device in self.data.items()
            if frame.targets(device.data.line, device.data.address, device.data.groups)
        ]

    def _update_motion(self, frame: ForwardFrame) -> bool:
        """Track Jalousie motion from the commands sent on the bus."""
        motion = COVER_MOTION_BY_OPCODE.get(frame.opcode)  # type: ignore[arg-type]
        # The DALI Jalousie ignores broadcast commands (factory default).
        if motion is None or frame.kind not in (TargetKind.SHORT, TargetKind.GROUP):
            return False
        changed = False
        for device_id in self._targeted(frame):
            device_motion = self.physical_motion(device_id, motion)
            if self.device_motion.get(device_id) is not device_motion:
                self.device_motion[device_id] = device_motion
                changed = True
        return changed

    def cover_inverted(self, device_id: int) -> bool:
        """Return True if the Jalousie is wired the other way round (UP closes)."""
        return str(device_id) in self.config_entry.options.get(CONF_INVERTED_COVERS, [])

    def physical_motion(self, device_id: int, motion: CoverMotion) -> CoverMotion:
        """Translate the motion implied by a DALI command into what the cover really does."""
        if not self.cover_inverted(device_id) or motion is CoverMotion.STOPPED:
            return motion
        return CoverMotion.CLOSING if motion is CoverMotion.OPENING else CoverMotion.OPENING

    @callback
    def _schedule_level_query(self, frame: ForwardFrame) -> None:
        """Query the real level after a dim burst the gateway does not track."""
        targets = [
            device_id
            for device_id in self._targeted(frame)
            if self.device_type(device_id) is EntityType.LIGHT
        ]
        if not targets:
            return
        self._dim_pending.update(targets)
        if self._dim_unsub:
            self._dim_unsub()
        self._dim_unsub = async_call_later(self.hass, DIM_SETTLE_DELAY, self._dim_settled)

    @callback
    def _dim_settled(self, _now: Any) -> None:
        self._dim_unsub = None
        device_ids, self._dim_pending = self._dim_pending, set()
        self.config_entry.async_create_background_task(
            self.hass, self._async_query_levels(device_ids), f"{DOMAIN} level query"
        )

    async def _async_query_levels(self, device_ids: set[int]) -> None:
        changed = False
        for device_id in sorted(device_ids):
            device = self.data.get(device_id)
            if device is None or not self.connected:
                continue
            level = await self.async_query_actual_level(device.data.line, device.data.address)
            raw = self._raw.get(device_id)
            if level is None or level == MASK or raw is None:
                continue
            features = raw.get("features") or {}
            delta: dict[str, Any] = {}
            if "switchable" in features:
                delta["switchable"] = {"status": level > 0}
            if "dimmable" in features:
                delta["dimmable"] = {"status": level_to_percent(level)}
            if delta:
                _LOGGER.debug("Device %s actual level after dimming: %s", device_id, level)
                self._raw[device_id] = deep_merge(raw, {"features": delta})
                self._rebuild({device_id})
                changed = True
        if changed:
            self.async_set_updated_data(self.data)

    async def async_send_frames(
        self, line: int, frames: list[list[int]], priority: int = COMMAND_PRIORITY
    ) -> None:
        """Send DALI forward frames as one sequence and check the gateway's results.

        The gateway answers every frame with a daliFrame result, but on real
        hardware ~2.5 % of the answers never arrive although the frame was put on
        the bus (seen in daliMonitor). A missing answer is therefore not an
        error; only a non-zero result code is.
        """
        if not frames:
            return
        async with self._send_lock:
            ws = self._ws
            if ws is None or ws.closed or not self.connected:
                raise HomeAssistantError(f"Not connected to the Lunatone gateway {self._ws_url}")
            futures: list[asyncio.Future[int | None]] = []
            try:
                for index, data in enumerate(frames):
                    if index:
                        # Back-to-back bursts got error results on real hardware;
                        # ~30 ms per frame is the bus speed anyway.
                        await asyncio.sleep(FRAME_SPACING)
                    future: asyncio.Future[int | None] = self.hass.loop.create_future()
                    self._frame_results.append(future)
                    futures.append(future)
                    await ws.send_json(_frame_message(line, data, priority, wait_for_answer=False))
                async with asyncio.timeout(FRAME_TIMEOUT + 0.05 * len(frames)):
                    await asyncio.gather(*futures, return_exceptions=True)
            except TimeoutError:
                _LOGGER.debug(
                    "%d of %d DALI frames not confirmed by the gateway",
                    sum(not future.done() for future in futures),
                    len(frames),
                )
            except (aiohttp.ClientError, ConnectionError) as err:
                raise HomeAssistantError(
                    f"Unable to send the DALI command to the gateway: {err!r}"
                ) from err
            finally:
                # Answers that never came must not be matched to later frames.
                for future in futures:
                    if not future.done():
                        future.cancel()
                        try:
                            self._frame_results.remove(future)
                        except ValueError:
                            pass
        answered = [future for future in futures if future.done() and not future.cancelled()]
        if any(future.exception() for future in answered):
            raise HomeAssistantError("Disconnected from the gateway while sending")
        if errors := [future.result() for future in answered if future.result() not in (0, None)]:
            raise HomeAssistantError(f"DALI command not sent by the gateway (result {errors[0]})")

    async def async_query_actual_level(self, line: int, address: int) -> int | None:
        """Send QUERY ACTUAL LEVEL to one short address over the WebSocket."""
        return await self._async_query(line, [short_address_byte(address), QUERY_ACTUAL_LEVEL])

    async def async_query_colour_value(
        self, line: int, address: int, selector: int
    ) -> int | None:
        """Read one DT8 colour value (DTR0 selector, e.g. 233 = red dim level)."""
        return await self._async_query(
            line,
            [short_address_byte(address), QUERY_COLOUR_VALUE],
            prefix=[[DTR0, selector], [ENABLE_DEVICE_TYPE, DT8]],
        )

    async def _async_query(
        self, line: int, query: list[int], prefix: list[list[int]] | None = None
    ) -> int | None:
        """Send optional setup frames and a query as one sequence; return the 8 bit answer."""
        frames = [*(prefix or []), query]
        async with self._send_lock:
            ws = self._ws
            if ws is None or ws.closed:
                return None
            answer: asyncio.Future[int | None] = self.hass.loop.create_future()
            self._pending_answer = (query, answer)
            results: list[asyncio.Future[int | None]] = []
            try:
                for data in frames:
                    result: asyncio.Future[int | None] = self.hass.loop.create_future()
                    self._frame_results.append(result)
                    results.append(result)
                    await ws.send_json(
                        _frame_message(line, data, QUERY_PRIORITY, wait_for_answer=data is query)
                    )
                async with asyncio.timeout(QUERY_TIMEOUT):
                    value = await answer
                # The per-frame results normally arrive before the answer.
                async with asyncio.timeout(0.3):
                    await asyncio.gather(*results, return_exceptions=True)
            except TimeoutError:
                if not answer.done():
                    _LOGGER.debug("No answer to %s on line %s", query, line)
                    return None
                return answer.result()
            except (aiohttp.ClientError, ConnectionError) as err:
                _LOGGER.debug("Query %s on line %s failed: %r", query, line, err)
                return None
            finally:
                self._pending_answer = None
                for result in results:
                    if not result.done():
                        result.cancel()
                        try:
                            self._frame_results.remove(result)
                        except ValueError:
                            pass
            return value

    async def _async_initial_sweep(self) -> None:
        """Read the real level and colour of every light once, after the first snapshot.

        The gateway's state is inferred from bus traffic it has seen; colours and
        levels set before it was installed (or while a lamp was off) are wrong in
        its snapshot. This is a one-off read, never repeated periodically.
        """
        read = corrected = 0
        for device_id in sorted(self.data):
            device = self.data.get(device_id)
            raw = self._raw.get(device_id)
            if device is None or raw is None or not self.connected:
                continue
            if self.device_type(device_id) is not EntityType.LIGHT:
                continue
            line, address = device.data.line, device.data.address
            features = raw.get("features") or {}
            delta: dict[str, Any] = {}
            level = await self.async_query_actual_level(line, address)
            if level is not None and level != MASK:
                if "switchable" in features:
                    delta["switchable"] = {"status": level > 0}
                if "dimmable" in features:
                    delta["dimmable"] = {"status": level_to_percent(level)}
            if "colorRGB" in features:
                rgb = {}
                for key, selector in (("r", 233), ("g", 234), ("b", 235)):
                    value = await self.async_query_colour_value(line, address, selector)
                    if value is not None and value != MASK:
                        rgb[key] = value / MAX_LEVEL
                if rgb:
                    delta["colorRGB"] = {"status": rgb}
            if "colorWAF" in features:
                value = await self.async_query_colour_value(line, address, 236)
                if value is not None and value != MASK:
                    delta["colorWAF"] = {"status": {"w": value / MAX_LEVEL}}
            if not delta:
                continue
            read += 1
            merged = deep_merge(raw, {"features": delta})
            if merged != raw:
                corrected += 1
                _LOGGER.debug("Initial read of device %s corrected: %s", device_id, delta)
            self._raw[device_id] = merged
            self._rebuild({device_id})
        _LOGGER.debug("Initial sweep: %d lights read, %d corrected", read, corrected)
        if corrected:
            self.async_set_updated_data(self.data)
        self.initial_sweep_done.set()

    @callback
    def _handle_frame_result(self, data: dict[str, Any]) -> None:
        if data.get("result") not in (None, 0):
            _LOGGER.debug("DALI frame not sent: %s", data)
        while self._frame_results:
            future = self._frame_results.popleft()
            if not future.done():
                future.set_result(data.get("result"))
                return

    @callback
    def _handle_answer(self, data: dict[str, Any]) -> None:
        pending = self._pending_answer
        if pending is None or data.get("queryData") != pending[0] or pending[1].done():
            return
        # result 8: 8 bit answer; 0: no answer; 63: framing error
        pending[1].set_result(data.get("daliData") if data.get("result") == 8 else None)

    @callback
    def _fail_pending(self) -> None:
        """Release everything waiting for the gateway after a disconnection."""
        while self._frame_results:
            future = self._frame_results.popleft()
            if not future.done():
                future.set_exception(ConnectionError("WebSocket disconnected"))
        if self._pending_answer and not self._pending_answer[1].done():
            self._pending_answer[1].set_result(None)

    # ----------------------------------------------------------- registry

    @callback
    def _remove_registry_device(self, device_id: int) -> None:
        registry = dr.async_get(self.hass)
        identifier = (DOMAIN, f"{self.config_entry.unique_id}-device{device_id}")
        if device := registry.async_get_device(identifiers={identifier}):
            _LOGGER.debug("Removing deleted DALI device %s", device_id)
            registry.async_update_device(
                device.id, remove_config_entry_id=self.config_entry.entry_id
            )

    # ------------------------------------------------------- types & groups

    def device_type(self, device_id: int) -> EntityType:
        """Return the entity type configured for a device."""
        configured = self.config_entry.options.get(CONF_DEVICE_TYPES, {}).get(str(device_id))
        if configured in EntityType.__members__.values():
            return EntityType(configured)
        device = self.data.get(device_id)
        return default_device_type(device.data) if device else EntityType.LIGHT

    def groups(self) -> dict[GroupKey, list[int]]:
        """Return the members of every DALI group in use, keyed by (line, group)."""
        groups: dict[GroupKey, list[int]] = {}
        for device_id, device in sorted(self.data.items()):
            for group in device.data.groups:
                groups.setdefault((device.data.line, group), []).append(device_id)
        return groups

    def group_type(self, key: GroupKey) -> EntityType:
        """Return the entity type configured for a DALI group."""
        configured = self.config_entry.options.get(CONF_GROUP_TYPES, {}).get(group_option_key(key))
        if configured in EntityType.__members__.values():
            return EntityType(configured)
        member_types = {
            self.device_type(device_id)
            for device_id in self.groups().get(key, [])
        } - {EntityType.DISABLED}
        if len(member_types) == 1:
            return member_types.pop()
        return EntityType.LIGHT

    def default_color(self, option_key: str) -> tuple[int, ...] | None:
        """Return the configured normal colour (R, G, B, W) of a device or group."""
        color = self.config_entry.options.get(CONF_DEFAULT_COLORS, {}).get(option_key)
        return tuple(color) if color else None


def _frame_message(
    line: int, data: list[int], priority: int, wait_for_answer: bool
) -> dict[str, Any]:
    return {
        "type": "daliFrame",
        "data": {
            "line": line,
            "numberOfBits": 16,
            "mode": {"sendTwice": False, "waitForAnswer": wait_for_answer, "priority": priority},
            "daliData": data,
        },
    }


def group_option_key(key: GroupKey) -> str:
    """Return the options key of a DALI group."""
    return f"{key[0]}-{key[1]}"


def device_color_key(device_id: int) -> str:
    """Return the default colour options key of a device."""
    return f"device{device_id}"


def group_color_key(key: GroupKey) -> str:
    """Return the default colour options key of a group."""
    return f"group{group_option_key(key)}"
