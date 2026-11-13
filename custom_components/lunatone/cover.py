"""Covers for the Lunatone DALI Jalousie (blinds/louvres driven by two relays).

The module moves up on RECALL MAX/UP, down on RECALL MIN/DOWN and stops on
OFF. It has no position feedback, so motion is taken from the commands seen
on the DALI bus (daliMonitor) and the position is unknown.
"""

from typing import Any, override

from homeassistant.components.cover import CoverEntity, CoverEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .commands import Target, down_frames, off_frames, up_frames
from .const import CoverMotion, EntityType
from .coordinator import LunatoneConfigEntry
from .entity import LunatoneDeviceEntity, LunatoneGroupEntity, async_setup_dynamic_entities

PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: LunatoneConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the Lunatone cover platform."""
    async_setup_dynamic_entities(
        config_entry,
        async_add_entities,
        EntityType.COVER,
        LunatoneCover,
        LunatoneGroupCover,
    )


class _JalousieCover(CoverEntity):
    _attr_assumed_state = True
    _attr_is_closed = None
    _attr_supported_features = (
        CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE | CoverEntityFeature.STOP
    )

    def _motion(self) -> CoverMotion | None:
        raise NotImplementedError

    @property
    @override
    def is_opening(self) -> bool:
        """Return True while the last command was a move up."""
        return self._motion() is CoverMotion.OPENING

    @property
    @override
    def is_closing(self) -> bool:
        """Return True while the last command was a move down."""
        return self._motion() is CoverMotion.CLOSING


class LunatoneCover(LunatoneDeviceEntity, _JalousieCover):
    """A DALI Jalousie module."""

    def _motion(self) -> CoverMotion | None:
        return self.coordinator.device_motion.get(self._device_id)

    @override
    async def async_open_cover(self, **kwargs: Any) -> None:
        """Open (DALI UP, or DOWN when the module is wired the other way round)."""
        if target := self.target:
            inverted = self.coordinator.cover_inverted(self._device_id)
            await self.async_send((down_frames if inverted else up_frames)(target))

    @override
    async def async_close_cover(self, **kwargs: Any) -> None:
        """Close (DALI DOWN, or UP when the module is wired the other way round)."""
        if target := self.target:
            inverted = self.coordinator.cover_inverted(self._device_id)
            await self.async_send((up_frames if inverted else down_frames)(target))

    @override
    async def async_stop_cover(self, **kwargs: Any) -> None:
        """Stop (DALI OFF)."""
        if target := self.target:
            await self.async_send(off_frames(target))


class LunatoneGroupCover(LunatoneGroupEntity, _JalousieCover):
    """A DALI group of Jalousie modules."""

    def _motion(self) -> CoverMotion | None:
        motions = {
            self.coordinator.device_motion.get(device.id)
            for device in self.members()
        }
        for motion in (CoverMotion.OPENING, CoverMotion.CLOSING):
            if motion in motions:
                return motion
        return CoverMotion.STOPPED if CoverMotion.STOPPED in motions else None

    async def _async_move(self, opening: bool) -> None:
        """Move all members: one group command unless members are wired differently.

        A group mixing normal and inverted modules would open some and close
        others with a single UP/DOWN, so each member then gets its own command.
        """
        members = self.members()
        inverted = {device.id: self.coordinator.cover_inverted(device.id) for device in members}
        if len(set(inverted.values())) <= 1:
            flip = next(iter(inverted.values()), False)
            await self.async_send((up_frames if opening != flip else down_frames)(self.target))
            return
        for device in members:
            target = Target.device(device.data.line, device.data.address)
            frames = up_frames if opening != inverted[device.id] else down_frames
            await self.coordinator.async_send_frames(device.data.line, frames(target))

    @override
    async def async_open_cover(self, **kwargs: Any) -> None:
        """Open all members."""
        await self._async_move(opening=True)

    @override
    async def async_close_cover(self, **kwargs: Any) -> None:
        """Close all members."""
        await self._async_move(opening=False)

    @override
    async def async_stop_cover(self, **kwargs: Any) -> None:
        """Stop all members."""
        await self.async_send(off_frames(self.target))
