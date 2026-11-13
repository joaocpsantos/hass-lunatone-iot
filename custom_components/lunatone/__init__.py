"""The Lunatone integration."""

import logging
import re
from typing import Final

from homeassistant.const import CONF_URL, Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr, entity_registry as er

from .config_flow import LunatoneConfigFlow
from .const import DOMAIN, MANUFACTURER, EntityType
from .coordinator import LunatoneConfigEntry, LunatoneCoordinator, LunatoneData

_LOGGER = logging.getLogger(__name__)
PLATFORMS: Final[list[Platform]] = [Platform.COVER, Platform.LIGHT, Platform.SWITCH]


async def _update_unique_id(
    hass: HomeAssistant, entry: LunatoneConfigEntry, new_unique_id: str
) -> None:
    _LOGGER.debug("Update unique ID")

    # Update all associated entities
    entity_registry = er.async_get(hass)
    entities = er.async_entries_for_config_entry(entity_registry, entry.entry_id)

    for entity in entities:
        parts = list(entity.unique_id.partition("-"))
        parts[0] = new_unique_id

        entity_registry.async_update_entity(
            entity.entity_id, new_unique_id="".join(parts)
        )

    # Update all associated devices
    device_registry = dr.async_get(hass)
    devices = dr.async_entries_for_config_entry(device_registry, entry.entry_id)

    for device in devices:
        identifier = device.identifiers.pop()
        parts = list(identifier[1].partition("-"))
        parts[0] = new_unique_id

        device_registry.async_update_device(
            device.id, new_identifiers={(identifier[0], "".join(parts))}
        )

    # Update the config entry itself
    hass.config_entries.async_update_entry(
        entry,
        unique_id=new_unique_id,
        minor_version=LunatoneConfigFlow.MINOR_VERSION,
        version=LunatoneConfigFlow.VERSION,
    )

    _LOGGER.debug("Update of unique ID successful")


async def async_setup_entry(hass: HomeAssistant, entry: LunatoneConfigEntry) -> bool:
    """Set up Lunatone from a config entry."""
    coordinator = LunatoneCoordinator(hass, entry)
    if not await coordinator.async_start():
        raise ConfigEntryNotReady(f"Unable to sync with {entry.data[CONF_URL]}")
    info_api = coordinator.info

    if info_api.data is None or info_api.serial_number is None:
        await coordinator.async_stop()
        raise ConfigEntryError(
            translation_domain=DOMAIN, translation_key="missing_device_info"
        )

    if info_api.uid is not None:
        new_unique_id = info_api.uid.replace("-", "")
        if new_unique_id != entry.unique_id:
            await _update_unique_id(hass, entry, new_unique_id)

    assert entry.unique_id

    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, entry.unique_id)},
        name=info_api.name,
        manufacturer=MANUFACTURER,
        sw_version=info_api.version,
        hw_version=info_api.data.device.pcb,
        configuration_url=entry.data[CONF_URL],
        serial_number=str(info_api.serial_number),
        model=info_api.product_name,
        model_id=(
            f"{info_api.data.device.article_number}{info_api.data.device.article_info}"
        ),
    )

    _async_remove_stale(hass, entry, coordinator)

    entry.runtime_data = LunatoneData(coordinator)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: LunatoneConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.coordinator.async_stop()
    return unloaded


async def _async_options_updated(hass: HomeAssistant, entry: LunatoneConfigEntry) -> None:
    """Reload when the device or group types change."""
    await hass.config_entries.async_reload(entry.entry_id)


@callback
def _async_remove_stale(
    hass: HomeAssistant, entry: LunatoneConfigEntry, coordinator: LunatoneCoordinator
) -> None:
    """Remove registry entries that no longer match the gateway or the chosen types.

    The device ids come from the gateway and can change (e.g. after a new
    scan), so the registry is reconciled with the current snapshot.
    """
    assert entry.unique_id
    uid = re.escape(entry.unique_id)
    groups = coordinator.groups()

    device_registry = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(device_registry, entry.entry_id):
        for domain, identifier in device.identifiers:
            match = re.fullmatch(rf"{uid}-device(\d+)", identifier)
            if domain == DOMAIN and match and int(match[1]) not in coordinator.data:
                _LOGGER.debug("Removing DALI device %s, no longer in the gateway", match[1])
                device_registry.async_update_device(
                    device.id, remove_config_entry_id=entry.entry_id
                )

    entity_registry = er.async_get(hass)
    for entity in er.async_entries_for_config_entry(entity_registry, entry.entry_id):
        if entity.domain == Platform.SENSOR:
            # Sensors were polled over REST; not supported in local push mode.
            expected: str | None = None
        elif match := re.fullmatch(rf"{uid}-device(\d+)", entity.unique_id):
            device_id = int(match[1])
            if device_id not in coordinator.data:
                continue
            expected = coordinator.device_type(device_id).value
        elif match := re.fullmatch(rf"{uid}-line(\d+)-group(\d+)", entity.unique_id):
            key = (int(match[1]), int(match[2]))
            entity_type = coordinator.group_type(key)
            expected = (
                entity_type.value
                if key in groups and entity_type is not EntityType.DISABLED
                else None
            )
        else:
            continue
        if entity.domain != expected:
            _LOGGER.debug("Removing %s (type changed or no longer present)", entity.entity_id)
            entity_registry.async_remove(entity.entity_id)
