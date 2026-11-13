"""A fake Lunatone DALI-2 IoT gateway (REST + WebSocket) for tests.

Behaves like the real gateway measured with tools/ws_probe.py: one global
timeSignature counter for REST responses and WebSocket events, an `info`
greeting on connect, partial `devices` deltas and daliFrame/daliAnswer replies.
"""

import asyncio
import copy
import time
from typing import Any

from aiohttp import web
from aiohttp.test_utils import TestServer

UID = "00000000-0000-4000-8000-000000000001"

INFO: dict[str, Any] = {
    "name": "DALI-2 IoT",
    "version": "v1.18.7/1.4.6",
    "tier": "plus",
    "uid": UID,
    "descriptor": {"lines": 1},
    "device": {
        "serial": 12345,
        "gtin": 9010342013607,
        "pcb": "2b",
        "articleNumber": 89453886,
        "articleInfo": "",
        "productionYear": 2020,
        "productionWeek": 1,
    },
    "lines": {
        "0": {
            "lineStatus": "ok",
            "device": {
                "serial": 12345,
                "gtin": 9010342013607,
                "pcb": "2b",
                "articleNumber": 89453886,
                "productionYear": 2020,
                "productionWeek": 1,
            },
        }
    },
}


def make_device(
    device_id: int,
    address: int,
    dali_types: list[int],
    groups: list[int] | None = None,
    rgbw: bool = False,
    on: bool = False,
    level: float = 0.0,
) -> dict[str, Any]:
    """Return a device as returned by GET /devices."""
    features: dict[str, Any] = {
        "switchable": {"status": on},
        "dimmable": {"status": level},
    }
    if rgbw:
        features["colorRGB"] = {"status": {"r": 1.0, "g": 1.0, "b": 1.0}}
        features["colorWAF"] = {"status": {"w": 0.0, "a": 0.0, "f": 0.0}}
    return {
        "id": device_id,
        "name": f"Line 0 - DALI#{address:02d}",
        "address": address,
        "line": 0,
        "type": "default",
        "available": True,
        "features": features,
        "scenes": [],
        "groups": groups or [],
        "daliTypes": dali_types,
    }


class FakeGateway:
    """In-process fake gateway."""

    def __init__(self) -> None:
        """Initialize with a small installation."""
        self.counter = 0
        self.devices: dict[int, dict[str, Any]] = {
            1: make_device(1, 0, [6], groups=[4]),
            2: make_device(2, 1, [8], groups=[4], rgbw=True),
            3: make_device(3, 2, [7]),
            4: make_device(4, 3, [0]),
        }
        self.clients: list[web.WebSocketResponse] = []
        self.received: list[dict[str, Any]] = []
        self.requests: list[tuple[str, str, Any]] = []
        # daliData of every daliFrame received, in order.
        self.frames: list[list[int]] = []
        # Number of upcoming frames left unconfirmed (real gateway: ~2.5 %).
        self.drop_confirmations = 0
        # Result code returned for the next frames (0 = sent).
        self.frame_result = 0
        # Set to hold GET /devices until released (race tests).
        self.devices_gate: asyncio.Event | None = None
        self.devices_requested = asyncio.Event()
        # Arc level answered to QUERY ACTUAL LEVEL, per short address.
        self.levels: dict[int, int] = {}
        # DT8 colour values answered to QUERY COLOUR VALUE: {address: {selector: value}}.
        self.colours: dict[int, dict[int, int]] = {}
        self.dtr0 = 0
        app = web.Application()
        app.add_routes(
            [
                web.get("/", self._ws),
                web.get("/devices", self._get_devices),
                web.get("/device/{id}", self._get_device),
                web.post("/device/{id}/control", self._control),
                web.post("/group/{id}/control", self._control),
                web.post("/broadcast/control", self._control),
            ]
        )
        self.server = TestServer(app, host="127.0.0.1")

    @property
    def url(self) -> str:
        """Return the base URL."""
        return f"http://127.0.0.1:{self.server.port}"

    async def start(self) -> None:
        """Start serving."""
        await self.server.start_server()

    async def close(self) -> None:
        """Stop serving."""
        await self.drop_clients()
        await self.server.close()

    def signature(self) -> dict[str, Any]:
        """Consume the next counter value."""
        self.counter += 1
        return {"timestamp": time.time(), "counter": self.counter}

    async def send(self, msg_type: str, data: dict[str, Any], counter: int | None = None) -> int:
        """Send an event to all clients; return its counter."""
        signature = self.signature()
        if counter is not None:
            signature["counter"] = counter
        message = {"type": msg_type, "data": data, "timeSignature": signature}
        for ws in list(self.clients):
            if not ws.closed:
                await ws.send_json(message)
        return signature["counter"]

    async def send_delta(
        self, device_id: int, delta: dict[str, Any], counter: int | None = None
    ) -> int:
        """Apply a partial device update and broadcast it as a `devices` event."""
        if device_id in self.devices:
            merged = copy.deepcopy(self.devices[device_id])
            for key, value in delta.items():
                if key == "features":
                    for feature, status in value.items():
                        merged["features"].setdefault(feature, {}).update(status)
                else:
                    merged[key] = value
            self.devices[device_id] = merged
        return await self.send("devices", {"devices": [{"id": device_id, **delta}]}, counter)

    async def send_monitor(self, frame: list[int], external: bool = True) -> None:
        """Report a DALI forward frame seen on the bus."""
        await self.send(
            "daliMonitor",
            {"bits": 16, "data": frame, "line": 0, "externalSource": external, "query": False},
        )

    async def drop_clients(self) -> None:
        """Close all WebSocket connections (network failure)."""
        for ws in list(self.clients):
            await ws.close()
        self.clients.clear()

    async def _ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"type": "info", "data": INFO, "timeSignature": self.signature()})
        self.clients.append(ws)
        async for msg in ws:
            message = msg.json()
            self.received.append(message)
            if message.get("type") == "daliFrame":
                await self._answer(ws, message["data"])
        return ws

    async def _answer(self, ws: web.WebSocketResponse, data: dict[str, Any]) -> None:
        self.frames.append(data["daliData"])
        if data["daliData"][0] == 0xA3:
            self.dtr0 = data["daliData"][1]
        if self.drop_confirmations:
            self.drop_confirmations -= 1
        else:
            await ws.send_json(
                {"type": "daliFrame", "data": {"line": data["line"], "result": self.frame_result}}
            )
        if not data["mode"]["waitForAnswer"]:
            return
        addr, opcode = data["daliData"]
        address = addr >> 1
        if opcode == 0xFA:
            level = self.colours.get(address, {}).get(self.dtr0)
        else:
            level = self.levels.get(address)
        await ws.send_json(
            {
                "type": "daliAnswer",
                "data": {
                    "line": data["line"],
                    "result": 8 if level is not None else 0,
                    "daliData": level,
                    "queryData": [addr, opcode],
                    "queryBits": 16,
                },
            }
        )

    async def _get_devices(self, request: web.Request) -> web.Response:
        self.devices_requested.set()
        # The snapshot reflects the state when the request was received.
        payload = {
            "devices": copy.deepcopy(list(self.devices.values())),
            "timeSignature": self.signature(),
        }
        if self.devices_gate is not None:
            await self.devices_gate.wait()
        return web.json_response(payload)

    async def _get_device(self, request: web.Request) -> web.Response:
        device = self.devices.get(int(request.match_info["id"]))
        if device is None:
            raise web.HTTPNotFound
        return web.json_response({**device, "timeSignature": self.signature()})

    async def _control(self, request: web.Request) -> web.Response:
        self.requests.append((request.method, request.path, await request.json()))
        return web.Response(status=204)
