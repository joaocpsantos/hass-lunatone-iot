# /// script
# requires-python = ">=3.12"
# dependencies = ["aiohttp>=3.9"]
# ///
"""Probe the WebSocket and REST behaviour of a Lunatone DALI-2 IoT gateway.

Answers, from real traffic:
- is timeSignature.counter global (shared by REST responses, WS events and
  all WS connections) or per resource / per connection?
- do `devices` events carry a timeSignature?
- do REST commands generate WS `devices` events, and how fast?
- is there any WS event for sensors?
- can GET /devices race with a concurrent command (initial snapshot problem)?
- how do RGBW commands behave (sequential vs single combined POST)?

Usage (from the repository root):
    uv run tools/ws_probe.py 192.0.2.10
    uv run tools/ws_probe.py 192.0.2.10 --device-id 12 --race --duration 300
    uv run tools/ws_probe.py 192.0.2.10 --rgbw-device-id 20

Every message is written to the console, to <log>.log and (raw JSON) to <log>.jsonl.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import sys
import time
from typing import Any, Callable
import uuid

import aiohttp

KNOWN_TYPES = {
    "info", "ping", "devices", "devicesDeleted", "scanProgress", "daliStatus",
    "daliMonitor", "daliFrame", "daliAnswer", "messageFlash", "datetime",
    "zones", "zonesDeleted", "sequences", "sequencesDeleted", "schedulers",
    "schedulersDeleted", "circadians", "circadiansDeleted", "triggerActions",
    "triggerActionsDeleted", "email",
}

_LOGGER = logging.getLogger("ws_probe")


@dataclass
class Event:
    """One WebSocket message as received."""

    idx: int
    conn: str
    t: float
    type: str
    counter: int | None
    gw_timestamp: float | None
    data: Any


@dataclass
class RestCall:
    """One REST request and its response."""

    method: str
    path: str
    t0: float
    t1: float
    status: int | None
    counter: int | None
    body: Any


@dataclass
class Results:
    """Findings collected for the final summary."""

    ping: list[str] = field(default_factory=list)
    toggle: list[str] = field(default_factory=list)
    race: list[str] = field(default_factory=list)
    rgbw: list[str] = field(default_factory=list)
    sensors: list[str] = field(default_factory=list)
    snapshot: list[str] = field(default_factory=list)
    per_device_signature: str = "?"


class Probe:
    """Holds connections, recorded traffic and the individual tests."""

    def __init__(self, args: argparse.Namespace, session: aiohttp.ClientSession, jsonl) -> None:
        self.args = args
        self.session = session
        self.jsonl = jsonl
        self.base = f"http://{args.host}"
        self.ws_url = f"ws://{args.host}/"
        self.t_start = time.monotonic()
        self.events: list[Event] = []
        self.rest_calls: list[RestCall] = []
        self.cond = asyncio.Condition()
        self.stop = asyncio.Event()
        self.sessions: dict[str, int] = {}
        self.results = Results()
        # Devices changed by a test and not yet restored (restored on Ctrl+C).
        self.to_restore: dict[int, dict] = {}

    # ------------------------------------------------------------------ utils

    def rel(self, t: float) -> str:
        return f"+{t - self.t_start:7.3f}s"

    def dump(self, kind: str, payload: dict) -> None:
        payload = {"kind": kind, "wall": datetime.now().isoformat(timespec="milliseconds"),
                   "t": round(time.monotonic() - self.t_start, 4), **payload}
        self.jsonl.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.jsonl.flush()

    async def wait_events(
        self, pred: Callable[[Event], bool], since: int, timeout: float, count: int = 1
    ) -> list[Event]:
        """Wait until `count` events after index `since` match `pred`."""
        deadline = time.monotonic() + timeout
        async with self.cond:
            while True:
                found = [e for e in self.events[since:] if pred(e)]
                remaining = deadline - time.monotonic()
                if len(found) >= count or remaining <= 0:
                    return found
                try:
                    await asyncio.wait_for(self.cond.wait(), remaining)
                except TimeoutError:
                    pass

    # -------------------------------------------------------------- websocket

    async def ws_reader(self, name: str, ready: asyncio.Event, reconnect: bool) -> None:
        while not self.stop.is_set():
            self.sessions[name] = self.sessions.get(name, 0) + 1
            label = f"{name}{self.sessions[name]}"
            try:
                async with self.session.ws_connect(
                    self.ws_url, heartbeat=self.args.heartbeat
                ) as ws:
                    _LOGGER.info("%s WS %s ligado a %s", self.rel(time.monotonic()), label, self.ws_url)
                    if not self.args.monitor:
                        await ws.send_json({"type": "filtering", "data": {"daliMonitor": True}})
                        _LOGGER.info("%s WS %s filtro enviado (daliMonitor=true)",
                                     self.rel(time.monotonic()), label)
                    ready.set()
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self.on_message(label, msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            _LOGGER.warning("WS %s erro: %s", label, ws.exception())
                        else:
                            _LOGGER.info("WS %s mensagem %s", label, msg.type)
                    _LOGGER.warning(
                        "%s WS %s fechado: code=%s exception=%r",
                        self.rel(time.monotonic()), label, ws.close_code, ws.exception(),
                    )
                    self.dump("ws_closed", {"conn": label, "code": ws.close_code,
                                            "exception": repr(ws.exception())})
            except (aiohttp.ClientError, OSError) as err:
                _LOGGER.warning("%s WS %s falhou: %r", self.rel(time.monotonic()), label, err)
            if not reconnect or self.stop.is_set():
                return
            await asyncio.sleep(2)
            _LOGGER.info("WS %s a religar...", name)

    async def on_message(self, conn: str, raw: str) -> None:
        now = time.monotonic()
        try:
            msg = json.loads(raw)
        except ValueError:
            _LOGGER.warning("%s WS %s texto não-JSON: %s", self.rel(now), conn, raw[:200])
            return
        sig = msg.get("timeSignature") or {}
        ev = Event(
            idx=len(self.events), conn=conn, t=now, type=str(msg.get("type")),
            counter=sig.get("counter"), gw_timestamp=sig.get("timestamp"),
            data=msg.get("data"),
        )
        async with self.cond:
            self.events.append(ev)
            self.cond.notify_all()
        self.dump("ws", {"conn": conn, "msg": msg})
        counter = f"#{ev.counter}" if ev.counter is not None else "#-(sem timeSignature)"
        _LOGGER.info("%s WS %-3s %-8s %-14s %s", self.rel(now), conn, counter, ev.type,
                     summarize_event(ev))

    # ------------------------------------------------------------------- rest

    async def rest(self, method: str, path: str, body: dict | None = None,
                   quiet: bool = False) -> RestCall:
        t0 = time.monotonic()
        status = None
        data: Any = None
        try:
            async with self.session.request(method, f"{self.base}/{path}", json=body) as resp:
                status = resp.status
                text = await resp.text()
                try:
                    data = json.loads(text) if text else None
                except ValueError:
                    data = text
        except (aiohttp.ClientError, TimeoutError) as err:
            data = repr(err)
        t1 = time.monotonic()
        counter = None
        if isinstance(data, dict):
            counter = (data.get("timeSignature") or {}).get("counter")
        call = RestCall(method, path, t0, t1, status, counter, data)
        self.rest_calls.append(call)
        self.dump("rest", {"method": method, "path": path, "request": body, "status": status,
                           "elapsed_ms": round((t1 - t0) * 1000, 1), "response": data})
        if not quiet or (status is not None and status >= 400):
            _LOGGER.info(
                "%s REST %s /%s %s -> %s em %.0f ms, counter=%s",
                self.rel(t0), method, path, json.dumps(body) if body else "", status,
                (t1 - t0) * 1000, counter,
            )
            if status is None or status >= 400:
                _LOGGER.warning("    resposta: %s", str(data)[:300])
        return call

    async def get_device(self, device_id: int) -> dict | None:
        call = await self.rest("GET", f"device/{device_id}", quiet=True)
        return call.body if isinstance(call.body, dict) and call.status == 200 else None

    async def control(self, device_id: int, body: dict) -> RestCall:
        return await self.rest("POST", f"device/{device_id}/control", body)

    def max_counter(self, conn_prefix: str = "A", before: float | None = None) -> int | None:
        values = [e.counter for e in self.events if e.conn.startswith(conn_prefix)
                  and e.counter is not None and (before is None or e.t <= before)]
        return max(values) if values else None

    # ------------------------------------------------------------------ tests

    async def inventory(self) -> list[dict]:
        call = await self.rest("GET", "devices")
        if not isinstance(call.body, dict):
            _LOGGER.error("GET /devices falhou; sem inventário")
            return []
        devices = call.body.get("devices", [])
        with_sig = sum(1 for d in devices if "timeSignature" in d)
        self.results.per_device_signature = f"{with_sig}/{len(devices)} devices com timeSignature próprio"
        _LOGGER.info("Inventário: %d devices (%s)", len(devices), self.results.per_device_signature)
        _LOGGER.info("  %-4s %-28s %-7s %-10s %s", "id", "nome", "linha/A", "daliTypes", "features")
        for d in devices:
            feats = d.get("features") or {}
            interesting = [f for f in ("switchable", "dimmable", "colorRGB", "colorWAF",
                                       "colorKelvin", "colorXY") if f in feats]
            _LOGGER.info("  %-4s %-28s %-7s %-10s %s", d.get("id"), str(d.get("name"))[:28],
                         f"{d.get('line')}/{d.get('address')}", d.get("daliTypes"),
                         ",".join(interesting))
        return devices

    async def sensors_snapshot(self) -> list[dict] | None:
        call = await self.rest("GET", "sensors")
        if call.status != 200 or not isinstance(call.body, dict):
            return None
        sensors = call.body.get("sensors", [])
        for s in sensors:
            _LOGGER.info("  sensor %-3s %-24s type=%-12s value=%s %s ts=%s", s.get("id"),
                         str(s.get("name"))[:24], s.get("type"), s.get("value"),
                         s.get("unit", ""), s.get("timestamp"))
        return sensors

    async def ping_test(self) -> None:
        """Compare REST and WS counters for one ping, seen by two connections."""
        ready_b = asyncio.Event()
        task_b = asyncio.create_task(self.ws_reader("B", ready_b, reconnect=False))
        try:
            await asyncio.wait_for(ready_b.wait(), 5)
        except TimeoutError:
            self.results.ping.append("2.ª ligação WS não abriu")
        await self.wait_events(lambda e: e.conn.startswith("B") and e.type == "info", 0, 3)
        info_a = next((e for e in self.events if e.conn.startswith("A") and e.type == "info"), None)
        info_b = next((e for e in self.events if e.conn.startswith("B") and e.type == "info"), None)
        if info_a and info_b:
            self.results.ping.append(
                f"counter do 'info' de saudação: ligação A=#{info_a.counter}, B=#{info_b.counter}"
            )

        token = uuid.uuid4().hex[:8]
        mark = len(self.events)
        call = await self.rest("POST", "ping/echo", {"echo": token})
        evs = await self.wait_events(
            lambda e: e.type == "ping" and isinstance(e.data, dict) and e.data.get("echo") == token,
            mark, 3, count=2,
        )
        seen = ", ".join(f"{e.conn}=#{e.counter} (+{(e.t - call.t0) * 1000:.0f} ms)" for e in evs)
        self.results.ping.append(
            f"POST /ping/echo: HTTP {call.status}, counter REST=#{call.counter}; eventos ping: {seen or 'nenhum'}"
        )
        counters = {e.counter for e in evs}
        if len(evs) == 2:
            self.results.ping.append(
                "mesmo counter nas duas ligações -> counter é da gateway, não por ligação"
                if len(counters) == 1 else
                "counters DIFERENTES nas duas ligações -> counter por ligação"
            )
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass

    async def snapshot_test(self, device_id: int | None) -> None:
        before = self.max_counter()
        call = await self.rest("GET", "devices")
        during = [e for e in self.events if e.conn.startswith("A") and call.t0 <= e.t <= call.t1]
        await asyncio.sleep(1)
        after = [e for e in self.events if e.conn.startswith("A") and e.t > call.t1 and e.counter is not None]
        self.results.snapshot.append(
            f"GET /devices counter=#{call.counter}; maior counter WS antes=#{before}; "
            f"eventos durante o pedido={len(during)}; 1.º counter WS depois="
            f"{'#' + str(after[0].counter) if after else 'nenhum'}"
        )
        if device_id is not None:
            call1 = await self.rest("GET", f"device/{device_id}")
            self.results.snapshot.append(f"GET /device/{device_id} counter=#{call1.counter}")
            call2 = await self.rest("GET", "devices")
            self.results.snapshot.append(
                f"GET /devices logo a seguir counter=#{call2.counter} "
                "(se sobe a cada GET, os pedidos REST também consomem counter)"
            )

    async def toggle_test(self, device_id: int) -> None:
        original = await self.get_device(device_id)
        if original is None:
            self.results.toggle.append(f"device {device_id} não encontrado")
            return
        self.to_restore[device_id] = original
        on = feature(original, "switchable")
        target = not bool(on)
        _LOGGER.info("Toggle: device %s está %s -> a enviar switchable=%s", device_id,
                     "ON" if on else "OFF", target)
        mark = len(self.events)
        call = await self.control(device_id, {"switchable": target})
        evs = await self.wait_events(lambda e: touches(e, device_id), mark, 5)
        if evs:
            e = evs[0]
            self.results.toggle.append(
                f"comando switchable={target}: HTTP {call.status} (counter REST=#{call.counter}); "
                f"evento devices #{e.counter} chegou {(e.t - call.t0) * 1000:.0f} ms após o POST: "
                f"{summarize_event(e)}"
            )
        else:
            self.results.toggle.append(
                f"comando switchable={target}: HTTP {call.status}; NENHUM evento devices em 5 s"
            )
        await asyncio.sleep(1)
        await self.restore(device_id, original)

    async def race_test(self, device_id: int) -> None:
        original = await self.get_device(device_id)
        if original is None:
            self.results.race.append(f"device {device_id} não encontrado")
            return
        self.to_restore[device_id] = original
        current = bool(feature(original, "switchable"))
        for rnd in range(self.args.race_rounds):
            delay = (rnd % 6) * 0.01
            target = not current
            mark = len(self.events)
            post_task = asyncio.create_task(
                self.rest("POST", f"device/{device_id}/control", {"switchable": target}, quiet=True)
            )
            await asyncio.sleep(delay)
            get_call = await self.rest("GET", "devices", quiet=True)
            post_call = await post_task
            await asyncio.sleep(1.5)
            snap_dev = next((d for d in get_call.body.get("devices", [])
                             if d.get("id") == device_id), {}) if isinstance(get_call.body, dict) else {}
            reflects = feature(snap_dev, "switchable") == target
            n = get_call.counter
            evs = [e for e in self.events[mark:]
                   if e.conn.startswith("A") and touches(e, device_id, "switchable")]
            ev = next((e for e in evs if device_delta(e, device_id).get("features", {})
                       .get("switchable", {}).get("status") == target), None)
            if ev is None:
                verdict = "sem evento de switchable"
            elif n is None or ev.counter is None:
                verdict = "sem counter"
            elif ev.counter > n and not reflects:
                verdict = "OK (evento mais recente que o snapshot; replay aplica-o)"
            elif ev.counter <= n and reflects:
                verdict = "OK (snapshot já inclui o evento; replay descarta-o)"
            elif ev.counter > n and reflects:
                verdict = "OK (snapshot já tem o valor; replay reaplica o mesmo valor)"
            else:
                verdict = "PROBLEMA: evento com counter <= snapshot mas snapshot sem a mudança"
            phase = "-"
            if ev is not None:
                phase = ("antes do GET" if ev.t < get_call.t0 else
                         "durante o GET" if ev.t <= get_call.t1 else "depois do GET")
            line = (
                f"ronda {rnd + 1} (atraso GET {delay * 1000:.0f} ms): POST HTTP {post_call.status} "
                f"counter=#{post_call.counter}; snapshot #{n} {'JÁ' if reflects else 'NÃO'} tem "
                f"switchable={target}; evento #{ev.counter if ev else '-'} chegou {phase} -> {verdict}"
            )
            _LOGGER.info("Race %s", line)
            self.results.race.append(line)
            current = target
            await asyncio.sleep(0.5)
        await self.restore(device_id, original)

    async def rgbw_test(self, device_id: int) -> None:
        original = await self.get_device(device_id)
        if original is None:
            self.results.rgbw.append(f"device {device_id} não encontrado")
            return
        feats = original.get("features") or {}
        if "colorRGB" not in feats or "colorWAF" not in feats:
            self.results.rgbw.append(
                f"device {device_id} não tem colorRGB+colorWAF (features: {', '.join(feats)})"
            )
            return
        variants: list[tuple[str, list[dict], tuple]] = [
            ("a) sequência da oficial (3 POSTs)",
             [{"colorWAF": {"w": 0.2}}, {"colorRGB": {"r": 1.0, "g": 0.0, "b": 0.0}},
              {"dimmable": 60}],
             ((1.0, 0.0, 0.0), 0.2, 60)),
            ("b) um POST com colorRGB+colorWAF+dimmable",
             [{"colorRGB": {"r": 0.0, "g": 1.0, "b": 0.0}, "colorWAF": {"w": 0.4}, "dimmable": 40}],
             ((0.0, 1.0, 0.0), 0.4, 40)),
            ("c) um POST com dimmableRGB+dimmableWAF",
             [{"dimmableRGB": {"r": 0.0, "g": 0.0, "b": 1.0, "dimmable": 80},
               "dimmableWAF": {"w": 0.6, "dimmable": 80}}],
             ((0.0, 0.0, 1.0), 0.6, 80)),
        ]
        self.to_restore[device_id] = original
        sleeps = [0.14, 0.16, 0.0]
        for name, bodies, (rgb, white, level) in variants:
            _LOGGER.info("RGBW %s", name)
            mark = len(self.events)
            statuses = []
            t0 = time.monotonic()
            for i, body in enumerate(bodies):
                call = await self.control(device_id, body)
                statuses.append(str(call.status))
                if len(bodies) > 1:
                    await asyncio.sleep(sleeps[i])
            await asyncio.sleep(2.5)
            evs = [e for e in self.events[mark:] if e.conn.startswith("A") and touches(e, device_id)]
            final = await self.get_device(device_id) or {}
            got_rgb = feature(final, "colorRGB") or {}
            got_w = (feature(final, "colorWAF") or {}).get("w")
            got_level = feature(final, "dimmable")
            ok = (
                all(close(got_rgb.get(k), v) for k, v in zip("rgb", rgb))
                and close(got_w, white) and close(got_level, level, 1.0)
            )
            seq = "; ".join(
                f"#{e.counter} +{(e.t - t0) * 1000:.0f}ms {summarize_device(device_delta(e, device_id))}"
                for e in evs
            )
            self.results.rgbw.append(
                f"{name}: HTTP {','.join(statuses)}; {len(evs)} eventos devices; "
                f"estado final {'OK' if ok else 'DIFERENTE'} (rgb={got_rgb} w={got_w} dim={got_level})"
                f"\n      eventos: {seq or '-'}"
            )
            await asyncio.sleep(1)
        await self.restore(device_id, original)

    async def restore(self, device_id: int, original: dict) -> None:
        """Best-effort restore of colours, level and on/off state."""
        feats = original.get("features") or {}
        if "colorWAF" in feats and isinstance(feature(original, "colorWAF"), dict):
            await self.control(device_id, {"colorWAF": feature(original, "colorWAF")})
            await asyncio.sleep(0.14)
        if "colorRGB" in feats and isinstance(feature(original, "colorRGB"), dict):
            await self.control(device_id, {"colorRGB": feature(original, "colorRGB")})
            await asyncio.sleep(0.16)
        if feature(original, "switchable"):
            level = feature(original, "dimmable")
            body = {"dimmable": level} if level else {"switchable": True}
        else:
            body = {"switchable": False}
        await self.control(device_id, body)
        self.to_restore.pop(device_id, None)
        _LOGGER.info("Estado original do device %s reposto (%s)", device_id, json.dumps(body))

    # ---------------------------------------------------------------- summary

    def summary(self, sensors_before: list[dict] | None, sensors_after: list[dict] | None) -> None:
        out = _LOGGER.info
        out("")
        out("=" * 78)
        out("RESUMO")
        out("=" * 78)
        by_type: dict[str, int] = {}
        for e in self.events:
            by_type[e.type] = by_type.get(e.type, 0) + 1
        out("Eventos por tipo: %s", ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())) or "-")
        dev = [e for e in self.events if e.type == "devices"]
        out("Eventos 'devices' com timeSignature: %d/%d",
            sum(1 for e in dev if e.counter is not None), len(dev))
        out("GET /devices: %s", self.results.per_device_signature)

        for label in sorted({e.conn for e in self.events}):
            seq = [e.counter for e in self.events if e.conn == label and e.counter is not None]
            if not seq:
                continue
            drops = sum(1 for a, b in zip(seq, seq[1:]) if b < a)
            dups = len(seq) - len(set(seq))
            gaps = sum(b - a - 1 for a, b in zip(seq, seq[1:]) if b > a + 1)
            out("Ligação %s: counters #%s..#%s, %d eventos, %d descidas, %d repetidos, %d buracos",
                label, seq[0], seq[-1], len(seq), drops, dups, gaps)

        ws_counters = {e.counter for e in self.events if e.conn.startswith("A") and e.counter is not None}
        rest = [c for c in self.rest_calls if c.counter is not None]
        if rest:
            shared = sum(1 for c in rest if c.counter in ws_counters)
            out("Respostas REST com counter: %d (%d com o mesmo valor de um evento WS)", len(rest), shared)
            out("  %s", ", ".join(f"{c.method} /{c.path}=#{c.counter}" for c in rest[:20]))
            if ws_counters:
                lo, hi = min(ws_counters), max(ws_counters)
                holes = set(range(lo, hi + 1)) - ws_counters
                others = {e.counter for e in self.events if not e.conn.startswith("A")}
                filled = holes & ({c.counter for c in rest} | others)
                out("Buracos na sequência da ligação A explicados por respostas REST ou pela ligação B: "
                    "%d/%d (perto de 100%% -> counter único e global)", len(filled), len(holes))

        for title, lines in (
            ("Ping / ligações", self.results.ping),
            ("Snapshot", self.results.snapshot),
            ("Comando REST -> evento", self.results.toggle),
            ("Race condition", self.results.race),
            ("RGBW", self.results.rgbw),
        ):
            if lines:
                out("%s:", title)
                for line in lines:
                    out("  - %s", line)

        unknown = sorted({e.type for e in self.events} - KNOWN_TYPES)
        out("Tipos de evento desconhecidos (candidatos a sensores): %s", ", ".join(unknown) or "nenhum")
        sensorish = sorted({e.type for e in self.events if "sensor" in e.type.lower()
                            or (isinstance(e.data, dict) and "sensors" in e.data)})
        out("Eventos com 'sensor' no tipo/dados: %s", ", ".join(sensorish) or "nenhum")
        if sensors_before is not None and sensors_after is not None:
            before = {s.get("id"): s.get("value") for s in sensors_before}
            changed = [f"{s.get('id')}: {before.get(s.get('id'))} -> {s.get('value')}"
                       for s in sensors_after if before.get(s.get("id")) != s.get("value")]
            out("Sensores (GET /sensors no início vs fim): %d, alterados: %s",
                len(sensors_after), "; ".join(changed) or "nenhum")
        elif sensors_before is None:
            out("GET /sensors indisponível nesta gateway")


# --------------------------------------------------------------------- helpers


def fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:g}"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def feature(device: dict, name: str) -> Any:
    feat = (device.get("features") or {}).get(name)
    return feat.get("status") if isinstance(feat, dict) else None


def device_delta(event: Event, device_id: int) -> dict:
    if not isinstance(event.data, dict):
        return {}
    return next((d for d in event.data.get("devices", []) if d.get("id") == device_id), {})


def touches(event: Event, device_id: int, feat: str | None = None) -> bool:
    if event.type != "devices":
        return False
    delta = device_delta(event, device_id)
    if not delta:
        return False
    return feat is None or feat in (delta.get("features") or {})


def close(a: Any, b: float, tol: float = 0.02) -> bool:
    return isinstance(a, (int, float)) and abs(a - b) <= tol


def summarize_device(d: dict) -> str:
    parts = []
    for key, value in d.items():
        if key == "id":
            continue
        if key == "features" and isinstance(value, dict):
            for fk, fv in value.items():
                parts.append(f"{fk}={fmt(fv.get('status', fv) if isinstance(fv, dict) else fv)}")
        elif key == "timeSignature" and isinstance(value, dict):
            parts.append(f"ts#{value.get('counter')}")
        else:
            parts.append(f"{key}={fmt(value)}")
    text = f"[{d.get('id')}] " + " ".join(parts)
    return text if len(text) <= 300 else text[:297] + "..."


def summarize_event(ev: Event) -> str:
    data = ev.data
    if ev.type == "devices" and isinstance(data, dict):
        return " | ".join(summarize_device(d) for d in data.get("devices", []))
    if ev.type == "info" and isinstance(data, dict):
        dev = data.get("device") or {}
        return f"name={data.get('name')} version={data.get('version')} serial={dev.get('serial')} lines={fmt(data.get('lines'))}"[:300]
    text = fmt(data)
    return text if len(text) <= 300 else text[:297] + "..."


# ------------------------------------------------------------------------ main


async def run(args: argparse.Namespace) -> None:
    stem = args.log or f"probe-{args.host.replace(':', '_')}-{datetime.now():%Y%m%d-%H%M%S}"
    file_handler = logging.FileHandler(f"{stem}.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(message)s", "%H:%M:%S"))
    _LOGGER.addHandler(file_handler)
    _LOGGER.info("Log: %s.log  JSON: %s.jsonl", stem, stem)

    timeout = aiohttp.ClientTimeout(total=10)
    with open(f"{stem}.jsonl", "w", encoding="utf-8") as jsonl:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            probe = Probe(args, session, jsonl)
            ready = asyncio.Event()
            reader = asyncio.create_task(probe.ws_reader("A", ready, reconnect=True))
            sensors_before = sensors_after = None
            try:
                try:
                    await asyncio.wait_for(ready.wait(), 15)
                except TimeoutError:
                    _LOGGER.error("Não foi possível ligar ao WebSocket em %s (gateway inacessível?)",
                                  probe.ws_url)
                    return
                await probe.wait_events(lambda e: e.type == "info", 0, 3)

                await probe.inventory()
                sensors_before = await probe.sensors_snapshot()
                await probe.ping_test()

                wait = args.snapshot_after - (time.monotonic() - probe.t_start)
                if wait > 0:
                    await asyncio.sleep(wait)
                await probe.snapshot_test(args.device_id)

                if args.device_id is not None:
                    await probe.toggle_test(args.device_id)
                    if args.race:
                        await probe.race_test(args.device_id)
                elif args.race:
                    _LOGGER.warning("--race precisa de --device-id; ignorado")
                if args.rgbw_device_id is not None:
                    await probe.rgbw_test(args.rgbw_device_id)

                remaining = args.duration - (time.monotonic() - probe.t_start)
                if remaining > 0:
                    _LOGGER.info(
                        "Testes automáticos concluídos. A registar ao vivo durante %.0f s: carrega "
                        "em interruptores, chama cenas/grupos, espera pelo scheduler... (Ctrl+C termina)",
                        remaining,
                    )
                    try:
                        await asyncio.wait_for(probe.stop.wait(), remaining)
                    except TimeoutError:
                        pass
                sensors_after = await probe.sensors_snapshot()
            except asyncio.CancelledError:
                _LOGGER.info("Interrompido")
            finally:
                for device_id, original in list(probe.to_restore.items()):
                    await probe.restore(device_id, original)
                probe.stop.set()
                reader.cancel()
                try:
                    await reader
                except asyncio.CancelledError:
                    pass
                probe.summary(sensors_before, sensors_after)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("host", help="IP (ou host[:porta]) da gateway")
    parser.add_argument("--device-id", type=int, help="luz para o teste de comando REST -> evento")
    parser.add_argument("--rgbw-device-id", type=int, help="luz RGBW para comparar formas de enviar cor")
    parser.add_argument("--race", action="store_true", help="teste da race condition do GET /devices (precisa de --device-id)")
    parser.add_argument("--race-rounds", type=int, default=10)
    parser.add_argument("--monitor", action="store_true", help="não filtrar eventos daliMonitor")
    parser.add_argument("--duration", type=float, default=120, help="duração total em segundos (default 120)")
    parser.add_argument("--snapshot-after", type=float, default=10, help="segundos até ao teste de snapshot")
    parser.add_argument("--heartbeat", type=float, default=30, help="heartbeat WS em segundos")
    parser.add_argument("--log", help="prefixo dos ficheiros de saída (sem extensão)")
    args = parser.parse_args()
    args.host = args.host.removeprefix("http://").removeprefix("ws://").rstrip("/")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(message)s", "%H:%M:%S"))
    _LOGGER.addHandler(console)
    _LOGGER.setLevel(logging.INFO)

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
