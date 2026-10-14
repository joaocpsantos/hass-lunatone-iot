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

- `iot_class: local_push`.
- State is written only from the WebSocket stream. REST is used for commands
  and for the state snapshot taken on every (re)connection.
- Same domain (`lunatone`), zeroconf discovery and unique IDs as core, so it
  replaces the core integration in place.

Work in progress. See `tools/ws_probe.py` for the protocol probe used to
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
