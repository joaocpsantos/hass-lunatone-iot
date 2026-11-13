"""Diagnostics support for Lunatone integration."""

from typing import Any

from homeassistant.core import HomeAssistant

from .coordinator import LunatoneConfigEntry, group_option_key


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: LunatoneConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data.coordinator
    return {
        "connected": coordinator.connected,
        "info": coordinator.info.data.model_dump() if coordinator.info.data else None,
        "options": dict(entry.options),
        "devices": [
            {
                **device.data.model_dump(),
                "entity_type": coordinator.device_type(device_id),
                "motion": coordinator.device_motion.get(device_id),
            }
            for device_id, device in coordinator.data.items()
        ],
        "groups": {
            group_option_key(key): {
                "members": members,
                "entity_type": coordinator.group_type(key),
            }
            for key, members in coordinator.groups().items()
        },
        "scan": coordinator.scan,
    }
