# hass-lunatone-iot

Custom component for Home Assistant that replaces the core `lunatone` integration
(Lunatone DALI-2 IoT gateways) with a **local push** version: device state is
received over the gateway's WebSocket instead of being polled over REST.

## Origin

Based on `homeassistant/components/lunatone` from Home Assistant core tag
**2026.7.1** (commit `209895db4a110d7792ce9533e24494e90a295ad0`). The first
commit of this repository is an untouched copy of that directory, so every
change made here can be reviewed with:

```
git diff <first commit> -- custom_components/lunatone
```

## Differences from the core integration

- `iot_class: local_push`, no polling at all.
- One WebSocket per gateway. State is written only from the WebSocket stream;
  REST is used only for the `GET /devices` snapshot on every (re)connection,
  combined with buffered events through the gateway's global `timeSignature`
  counter.
- Commands are sent as DALI frames over the same WebSocket (`daliFrame`).
  Colour lights get RGB, white and level in one sequence applied by a single
  DAPC (no intermediate colour).
- The entity type of each DALI device and DALI group (light, cover for the
  Lunatone DALI Jalousie, switch for DT7 relays) is chosen per gateway in the
  integration options. DALI groups are exposed as entities.
- Bus traffic from other controllers is followed through `daliMonitor`:
  Jalousie motion, and the real level is queried after UP/DOWN dimming (which
  the gateway does not track).
- Optional "normal colour" per colour light/group, used when it is switched on
  without a colour.
- Same domain (`lunatone`), zeroconf discovery and unique IDs as core, so it
  replaces the core integration in place. The polled sensor platform is not
  available.

See `tools/ws_probe.py` for the protocol probe used to
validate the WebSocket behaviour.

## Installation (HACS)

1. HACS → Integrations → ⋮ → Custom repositories → add this repository
   (category: Integration).
2. Install **Lunatone DALI-2 IoT (local push)** and restart Home Assistant.
3. Add one Lunatone entry per gateway (discovered via zeroconf, or manually by URL).

Requires Home Assistant 2026.7.0 or later.

## Debug logging

```yaml
logger:
  default: warning
  logs:
    custom_components.lunatone: debug
```

## Protocol probe

`tools/ws_probe.py` is a standalone script (only depends on `aiohttp`) that
connects to a gateway's WebSocket, logs every event with its `timeSignature`
and runs a few REST checks. Run it from the repository root:

```
uv run tools/ws_probe.py <gateway-ip> --help
```

## Development

Tests use `pytest-homeassistant-custom-component` pinned to Home Assistant 2026.7.1
and a fake gateway (`tests/fake_gateway.py`):

```
uv venv --python 3.14 .venv
uv pip install --python .venv "pytest-homeassistant-custom-component==0.13.345" "lunatone-rest-api-client==0.9.2"
.venv/Scripts/python -m pytest      # Linux: .venv/bin/python -m pytest
```

On Windows the Home Assistant test harness imports the Linux-only `fcntl` and
`resource` modules; add `tests/windows_shims` to `PYTHONPATH` (test use only).
