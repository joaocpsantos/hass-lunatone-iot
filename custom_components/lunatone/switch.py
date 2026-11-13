"""Switches for DALI switching actuators (DT7 relays such as the DALI-2 RM16)."""

from typing import Any, override

from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .commands import off_frames, on_frames
from .const import EntityType
from .coordinator import LunatoneConfigEntry
from .entity import LunatoneDeviceEntity, LunatoneGroupEntity, async_setup_dynamic_entities

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: LunatoneConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the Lunatone switch platform."""
    async_setup_dynamic_entities(
        config_entry,
        async_add_entities,
        EntityType.SWITCH,
        LunatoneSwitch,
        LunatoneGroupSwitch,
    )


class LunatoneSwitch(LunatoneDeviceEntity, SwitchEntity):
    """A DALI relay."""

    @property
    @override
    def is_on(self) -> bool | None:
        """Return True if the relay is on."""
        return self.device.is_on if self.device else None

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Switch on (RECALL MAX)."""
        if target := self.target:
            await self.async_send(on_frames(target))

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Switch off (OFF)."""
        if target := self.target:
            await self.async_send(off_frames(target))


class LunatoneGroupSwitch(LunatoneGroupEntity, SwitchEntity):
    """A DALI group of relays."""

    @property
    @override
    def is_on(self) -> bool:
        """Return True if any member is on."""
        return any(device.is_on for device in self.members())

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Switch all members on."""
        await self.async_send(on_frames(self.target))

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Switch all members off."""
        await self.async_send(off_frames(self.target))
