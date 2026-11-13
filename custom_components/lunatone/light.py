"""Platform for Lunatone light integration."""

from typing import Any, override

from lunatone_rest_api_client import Device
from lunatone_rest_api_client.models import LineStatus

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_RGB_COLOR,
    ATTR_RGBW_COLOR,
    ColorMode,
    LightEntity,
    brightness_supported,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util.color import brightness_to_value, value_to_brightness

from .commands import Frame, Target, color_frames, level_frames, off_frames, on_frames
from .const import DOMAIN, EntityType
from .coordinator import (
    LunatoneConfigEntry,
    LunatoneCoordinator,
    device_color_key,
    group_color_key,
)
from .entity import (
    LunatoneDeviceEntity,
    LunatoneGroupEntity,
    async_setup_dynamic_entities,
    average,
)

PARALLEL_UPDATES = 0
BRIGHTNESS_SCALE = (1, 100)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: LunatoneConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the Lunatone Light platform."""
    coordinator = config_entry.runtime_data.coordinator
    assert coordinator.info.data is not None

    async_add_entities(
        LunatoneLineBroadcastLight(coordinator, int(line))
        for line in coordinator.info.data.lines
    )
    async_setup_dynamic_entities(
        config_entry,
        async_add_entities,
        EntityType.LIGHT,
        LunatoneLight,
        LunatoneGroupLight,
    )


def _turn_on_frames(
    target: Target, kwargs: dict[str, Any], percent: float, is_on: bool
) -> list[Frame]:
    """Frames for turn_on: colour and level in one sequence, applied by a single DAPC."""
    rgb: tuple[float, float, float] | None = None
    white: float | None = None
    if ATTR_RGBW_COLOR in kwargs:
        red, green, blue, white = (value / 255 for value in kwargs[ATTR_RGBW_COLOR])
        rgb = (red, green, blue)
    elif ATTR_RGB_COLOR in kwargs:
        red, green, blue = (value / 255 for value in kwargs[ATTR_RGB_COLOR])
        rgb = (red, green, blue)
    kelvin = kwargs.get(ATTR_COLOR_TEMP_KELVIN)
    if rgb is not None or kelvin is not None:
        return color_frames(target, percent, rgb=rgb, white=white, kelvin=kelvin)
    if ATTR_BRIGHTNESS in kwargs or not is_on:
        return level_frames(target, percent)
    return []


def _with_default_color(
    kwargs: dict[str, Any],
    is_on: bool,
    color_mode: ColorMode,
    default: tuple[int, ...] | None,
) -> dict[str, Any]:
    """Add the configured normal colour when a colour light is switched on without one.

    The colour reported by the gateway is unreliable (it does not track colours
    set by other controllers), so it is never used as the colour to restore.
    """
    if is_on or default is None or ATTR_RGB_COLOR in kwargs or ATTR_RGBW_COLOR in kwargs:
        return kwargs
    # The normal colour is configured in DALI levels (0-254); HA colours are 0-255.
    color = tuple(round(value * 255 / 254) for value in default)
    if color_mode is ColorMode.RGBW:
        return {**kwargs, ATTR_RGBW_COLOR: color}
    if color_mode is ColorMode.RGB:
        return {**kwargs, ATTR_RGB_COLOR: color[:3]}
    return kwargs


def _color_mode(device: Device) -> ColorMode:
    if device.rgbw_color is not None:
        return ColorMode.RGBW
    if device.rgb_color is not None:
        return ColorMode.RGB
    if device.color_temperature is not None:
        return ColorMode.COLOR_TEMP
    if device.brightness is not None:
        return ColorMode.BRIGHTNESS
    return ColorMode.ONOFF


def _to_rgb(color: tuple[float, ...] | None) -> tuple[int, ...] | None:
    return color and tuple(round(value * 255) for value in color)


class LunatoneLight(LunatoneDeviceEntity, LightEntity):
    """Representation of a Lunatone light."""

    BRIGHTNESS_SCALE = BRIGHTNESS_SCALE

    _last_brightness = 255

    _attr_min_color_temp_kelvin = 1000
    _attr_max_color_temp_kelvin = 10000

    def __init__(self, coordinator: LunatoneCoordinator, device_id: int) -> None:
        """Initialize a Lunatone light."""
        super().__init__(coordinator, device_id)
        self._device = self.coordinator.data[device_id]

    @property
    @override
    def is_on(self) -> bool:
        """Return True if light is on."""
        return self._device.is_on

    @property
    @override
    def brightness(self) -> int | None:
        """Return the brightness of this light between 0..255."""
        return (
            value_to_brightness(self.BRIGHTNESS_SCALE, self._device.brightness)
            if self._device.brightness is not None
            else None
        )

    @property
    @override
    def color_mode(self) -> ColorMode:
        """Return the color mode of the light."""
        return _color_mode(self._device)

    @property
    @override
    def supported_color_modes(self) -> set[ColorMode]:
        """Return the supported color modes."""
        return {self.color_mode}

    @property
    @override
    def color_temp_kelvin(self) -> int | None:
        """Return the color temp of this light in kelvin."""
        return self._device.color_temperature

    @property
    @override
    def rgb_color(self) -> tuple[int, int, int] | None:
        """Return the RGB color of this light."""
        return _to_rgb(self._device.rgb_color)  # type: ignore[return-value]

    @property
    @override
    def rgbw_color(self) -> tuple[int, int, int, int] | None:
        """Return the RGBW color of this light."""
        return _to_rgb(self._device.rgbw_color)  # type: ignore[return-value]

    @callback
    @override
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        # Keep the last known object when the device disappears (unavailable).
        self._device = self.coordinator.data.get(self._device_id, self._device)
        self.async_write_ha_state()

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Instruct the light to turn on.

        State is not updated here: the gateway reports the change over the WebSocket.
        """
        kwargs = _with_default_color(
            kwargs,
            self.is_on,
            self.color_mode,
            self.coordinator.default_color(device_color_key(self._device_id)),
        )
        target = Target.device(self._device.data.line, self._device.data.address)
        if brightness_supported(self.supported_color_modes):
            percent = brightness_to_value(
                self.BRIGHTNESS_SCALE,
                kwargs.get(
                    ATTR_BRIGHTNESS,
                    self.brightness if self.is_on and self.brightness else self._last_brightness,
                ),
            )
            await self.async_send(_turn_on_frames(target, kwargs, percent, self.is_on))
        else:
            await self.async_send(on_frames(target))

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Instruct the light to turn off."""
        target = Target.device(self._device.data.line, self._device.data.address)
        if brightness_supported(self.supported_color_modes):
            if self.brightness:
                self._last_brightness = self.brightness
            # DAPC 0: switches off with the configured fade time, like the official integration.
            await self.async_send(level_frames(target, 0))
        else:
            await self.async_send(off_frames(target))


class LunatoneGroupLight(LunatoneGroupEntity, LightEntity):
    """A DALI group of lights, controlled with one command on the bus."""

    _last_brightness = 255

    def _lights(self) -> list[Device]:
        return self.members(EntityType.LIGHT)

    @property
    @override
    def is_on(self) -> bool:
        """Return True if any member light is on."""
        return any(device.is_on for device in self._lights())

    @property
    @override
    def brightness(self) -> int | None:
        """Return the mean brightness of the member lights that are on."""
        level = average(
            device.brightness
            for device in self._lights()
            if device.is_on and device.brightness is not None
        )
        return value_to_brightness(BRIGHTNESS_SCALE, level) if level else None

    @property
    @override
    def color_mode(self) -> ColorMode:
        """Return RGBW if any member is RGBW, otherwise the simplest common mode."""
        modes = {_color_mode(device) for device in self._lights()}
        for mode in (ColorMode.RGBW, ColorMode.RGB, ColorMode.BRIGHTNESS):
            if mode in modes:
                return mode
        if ColorMode.COLOR_TEMP in modes:
            return ColorMode.BRIGHTNESS
        return ColorMode.ONOFF

    @property
    @override
    def supported_color_modes(self) -> set[ColorMode]:
        """Return the supported color modes."""
        return {self.color_mode}

    def _colored(self) -> list[Device]:
        lights = [device for device in self._lights() if device.rgb_color is not None]
        return [device for device in lights if device.is_on] or lights

    @property
    @override
    def rgb_color(self) -> tuple[int, int, int] | None:
        """Return the RGB color of the first coloured member."""
        colored = self._colored()
        return _to_rgb(colored[0].rgb_color) if colored else None  # type: ignore[return-value]

    @property
    @override
    def rgbw_color(self) -> tuple[int, int, int, int] | None:
        """Return the RGBW color of the first RGBW member."""
        colored = [device for device in self._colored() if device.rgbw_color is not None]
        return _to_rgb(colored[0].rgbw_color) if colored else None  # type: ignore[return-value]

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the group on with one command per action on the bus."""
        kwargs = _with_default_color(
            kwargs,
            self.is_on,
            self.color_mode,
            self.coordinator.default_color(group_color_key(self._key)),
        )
        if not brightness_supported(self.supported_color_modes):
            await self.async_send(on_frames(self.target))
            return
        percent = brightness_to_value(
            BRIGHTNESS_SCALE,
            kwargs.get(
                ATTR_BRIGHTNESS,
                self.brightness if self.is_on and self.brightness else self._last_brightness,
            ),
        )
        # Members without colour support ignore the DT8 commands but follow the DAPC.
        await self.async_send(_turn_on_frames(self.target, kwargs, percent, self.is_on))

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the group off."""
        if self.brightness:
            self._last_brightness = self.brightness
        await self.async_send(level_frames(self.target, 0))


class LunatoneLineBroadcastLight(CoordinatorEntity[LunatoneCoordinator], LightEntity):
    """Representation of a Lunatone line broadcast light."""

    BRIGHTNESS_SCALE = BRIGHTNESS_SCALE

    _attr_assumed_state = True
    _attr_color_mode = ColorMode.BRIGHTNESS
    _attr_has_entity_name = True
    _attr_name = None
    _attr_supported_color_modes = {ColorMode.BRIGHTNESS}

    def __init__(self, coordinator: LunatoneCoordinator, line: int) -> None:
        """Initialize a Lunatone line broadcast light."""
        super().__init__(coordinator)
        self._line = line
        config_entry_unique_id = coordinator.config_entry.unique_id
        info = coordinator.info.data
        assert info is not None

        self._attr_unique_id = f"{config_entry_unique_id}-line{line}"

        line_device = info.lines[str(line)].device
        extra_info: dict = {}
        if line_device.serial != info.device.serial:
            extra_info.update(
                serial_number=str(line_device.serial),
                hw_version=line_device.pcb,
                model_id=f"{line_device.article_number}{line_device.article_info}",
            )

        assert self.unique_id
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, self.unique_id)},
            name=f"DALI Line {line}",
            via_device=(DOMAIN, config_entry_unique_id),
            **extra_info,
        )

    @property
    @override
    def available(self) -> bool:
        """Return True if entity is available."""
        info = self.coordinator.info.data
        line = info.lines.get(str(self._line)) if info else None
        return (
            super().available
            and self.coordinator.connected
            and line is not None
            and line.line_status == LineStatus.OK
        )

    @override
    async def async_turn_on(self, **kwargs: Any) -> None:
        """Instruct the line to turn on."""
        percent = brightness_to_value(self.BRIGHTNESS_SCALE, kwargs.get(ATTR_BRIGHTNESS, 255))
        await self.coordinator.async_send_frames(
            self._line, level_frames(Target.broadcast(self._line), percent)
        )

    @override
    async def async_turn_off(self, **kwargs: Any) -> None:
        """Instruct the line to turn off."""
        await self.coordinator.async_send_frames(
            self._line, level_frames(Target.broadcast(self._line), 0)
        )
