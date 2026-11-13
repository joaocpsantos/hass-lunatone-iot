"""Base entities and dynamic entity setup for Lunatone."""

from collections.abc import Callable, Iterable
from typing import override

from lunatone_rest_api_client import Device

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .commands import Frame, Target
from .const import DOMAIN, EntityType
from .coordinator import GroupKey, LunatoneConfigEntry, LunatoneCoordinator


@callback
def async_register_device(
    hass: HomeAssistant,
    entry: ConfigEntry,
    identifier: str,
    parent_identifier: str,
    **info: str,
) -> None:
    """Create or update a registry device and link it to its parent device.

    The parent link is set with via_device_id here instead of via_device in the
    entities' DeviceInfo: Home Assistant rejects via_device when it re-adds an
    entity after a registry change (entity_id rename, re-enable).
    """
    registry = dr.async_get(hass)
    device = registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, identifier)}, **info
    )
    parent = next(
        (
            entry_device
            for entry_device in dr.async_entries_for_config_entry(registry, entry.entry_id)
            if (DOMAIN, parent_identifier) in entry_device.identifiers
        ),
        None,
    )
    if parent is not None and device.via_device_id != parent.id:
        registry.async_update_device(device.id, via_device_id=parent.id)


class LunatoneDeviceEntity(CoordinatorEntity[LunatoneCoordinator]):
    """Entity for one DALI device registered in the gateway."""

    _attr_has_entity_name = True
    _attr_name = None
    _attr_should_poll = False

    def __init__(self, coordinator: LunatoneCoordinator, device_id: int) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        assert coordinator.config_entry.unique_id
        config_entry_unique_id = coordinator.config_entry.unique_id
        device = coordinator.data[device_id]
        self._device_id = device_id
        self._attr_unique_id = f"{config_entry_unique_id}-device{device_id}"
        async_register_device(
            coordinator.hass,
            coordinator.config_entry,
            self._attr_unique_id,
            f"{config_entry_unique_id}-line{device.data.line}",
            name=device.name,
        )
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, self._attr_unique_id)},
            name=device.name,
        )

    @property
    def device(self) -> Device | None:
        """Return the current library device object."""
        return self.coordinator.data.get(self._device_id)

    async def async_send(self, frames: list[Frame]) -> None:
        """Send DALI frames to this device's short address."""
        if (device := self.device) is None:
            return
        target = Target.device(device.data.line, device.data.address)
        await self.coordinator.async_send_frames(target.line, frames)

    @property
    def target(self) -> Target | None:
        """Return the DALI short address of the device."""
        device = self.device
        return Target.device(device.data.line, device.data.address) if device else None

    @property
    @override
    def available(self) -> bool:
        """Return True if the gateway is connected and the device exists."""
        return super().available and self.coordinator.connected and self.device is not None


class LunatoneGroupEntity(CoordinatorEntity[LunatoneCoordinator]):
    """Entity for one DALI group (as defined on the bus, e.g. by another controller)."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, coordinator: LunatoneCoordinator, key: GroupKey) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        assert coordinator.config_entry.unique_id
        config_entry_unique_id = coordinator.config_entry.unique_id
        line, number = key
        self._key = key
        self._attr_unique_id = f"{config_entry_unique_id}-line{line}-group{number}"
        self._attr_name = f"Group {number}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{config_entry_unique_id}-line{line}")},
        )

    @property
    def target(self) -> Target:
        """Return the DALI group address."""
        return Target.group(*self._key)

    async def async_send(self, frames: list[Frame]) -> None:
        """Send DALI frames to the group."""
        await self.coordinator.async_send_frames(self._key[0], frames)

    def members(self, entity_type: EntityType | None = None) -> list[Device]:
        """Return the member devices, optionally only those of one entity type."""
        return [
            self.coordinator.data[device_id]
            for device_id in self.coordinator.groups().get(self._key, [])
            if entity_type is None or self.coordinator.device_type(device_id) is entity_type
        ]

    @property
    @override
    def available(self) -> bool:
        """Return True if the gateway is connected and the group has members."""
        return (
            super().available
            and self.coordinator.connected
            and self._key in self.coordinator.groups()
        )


@callback
def async_setup_dynamic_entities(
    config_entry: LunatoneConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
    entity_type: EntityType,
    device_factory: Callable[[LunatoneCoordinator, int], Entity] | None,
    group_factory: Callable[[LunatoneCoordinator, GroupKey], Entity] | None,
) -> None:
    """Add entities for devices and groups of one type, now and when new ones appear."""
    coordinator = config_entry.runtime_data.coordinator
    known_devices: set[int] = set()
    known_groups: set[GroupKey] = set()

    @callback
    def _async_check() -> None:
        new: list[Entity] = []
        if device_factory:
            known_devices.intersection_update(coordinator.data)
            for device_id in coordinator.data:
                if device_id in known_devices:
                    continue
                if coordinator.device_type(device_id) is entity_type:
                    known_devices.add(device_id)
                    new.append(device_factory(coordinator, device_id))
        if group_factory:
            # Group entities stay (unavailable) when a group loses all members.
            for key in coordinator.groups():
                if key not in known_groups and coordinator.group_type(key) is entity_type:
                    known_groups.add(key)
                    new.append(group_factory(coordinator, key))
        if new:
            async_add_entities(new)

    _async_check()
    config_entry.async_on_unload(coordinator.async_add_listener(_async_check))


def average(values: Iterable[float]) -> float | None:
    """Return the mean of the values, or None if there are none."""
    values = list(values)
    return sum(values) / len(values) if values else None
