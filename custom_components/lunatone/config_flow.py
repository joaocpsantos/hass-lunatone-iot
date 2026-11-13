"""Config flow for Lunatone."""

from typing import Any, Final, override

import aiohttp
from lunatone_rest_api_client import Auth, Info
import voluptuous as vol
from yarl import URL

from homeassistant.config_entries import (
    SOURCE_RECONFIGURE,
    ConfigEntry,
    ConfigEntryState,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_NAME, CONF_URL
from homeassistant.core import callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    SelectSelector,
    SelectSelectorConfig,
    BooleanSelector,
    SelectSelectorMode,
    TextSelector,
)
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo

from .const import (
    CONF_DEFAULT_COLORS,
    CONF_DEVICE_TYPES,
    CONF_GROUP_TYPES,
    CONF_INVERTED_COVERS,
    DOMAIN,
    EntityType,
)
from .coordinator import device_color_key, group_color_key, group_option_key

DATA_SCHEMA: Final[vol.Schema] = vol.Schema(
    {vol.Required(CONF_URL, default="http://"): cv.string},
)


class LunatoneConfigFlow(ConfigFlow, domain=DOMAIN):
    """Lunatone config flow."""

    VERSION = 1
    MINOR_VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._data: dict[str, Any] = {}

    @staticmethod
    @callback
    @override
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow (entity type per DALI device and group)."""
        return LunatoneOptionsFlow()

    @override
    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a flow initialized by the user."""
        errors: dict[str, str] = {}
        if user_input is not None:
            url = URL(user_input[CONF_URL]).human_repr()[:-1]
            data = {CONF_URL: url}
            self._async_abort_entries_match(data)
            auth_api = Auth(
                session=async_get_clientsession(self.hass),
                base_url=url,
            )
            info_api = Info(auth_api)
            try:
                await info_api.async_update()
            except aiohttp.InvalidUrlClientError:
                errors["base"] = "invalid_url"
            except aiohttp.ClientConnectionError:
                errors["base"] = "cannot_connect"
            else:
                if info_api.serial_number is None:
                    errors["base"] = "missing_device_info"
                else:
                    unique_id = str(info_api.serial_number)
                    if info_api.uid is not None:
                        unique_id = info_api.uid.replace("-", "")
                    await self.async_set_unique_id(unique_id)
                    if self.source == SOURCE_RECONFIGURE:
                        self._abort_if_unique_id_mismatch()
                        return self.async_update_reload_and_abort(
                            self._get_reconfigure_entry(), data_updates=data, title=url
                        )
                    self._abort_if_unique_id_configured()
                    return self.async_create_entry(title=url, data=data)
        return self.async_show_form(
            step_id="user", data_schema=DATA_SCHEMA, errors=errors
        )

    @override
    async def async_step_zeroconf(
        self, discovery_info: ZeroconfServiceInfo
    ) -> ConfigFlowResult:
        """Handle a flow initialized by zeroconf discovery."""
        url = URL.build(scheme="http", host=discovery_info.host).human_repr()[:-1]
        uid = discovery_info.properties["uid"]
        await self.async_set_unique_id(uid.replace("-", ""))
        self._abort_if_unique_id_configured(updates={CONF_URL: url})

        auth_api = Auth(
            session=async_get_clientsession(self.hass),
            base_url=url,
        )
        info_api = Info(auth_api)

        try:
            await info_api.async_update()
        except aiohttp.InvalidUrlClientError:
            return self.async_abort(reason="invalid_url")
        except aiohttp.ClientConnectionError:
            return self.async_abort(reason="cannot_connect")

        self._data[CONF_URL] = url

        self.context["title_placeholders"] = {
            "model": discovery_info.properties["device"],
            "name": discovery_info.name.rsplit(" ", maxsplit=1)[0],
        }

        return await self.async_step_discovery_confirm()

    async def async_step_discovery_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm the discovered device."""
        if user_input is not None:
            return self.async_create_entry(title=self._data[CONF_URL], data=self._data)
        return self.async_show_form(
            step_id="discovery_confirm",
            description_placeholders=self._data,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a reconfiguration flow initialized by the user."""
        if user_input is not None:
            return await self.async_step_user(user_input)

        entry = self._get_reconfigure_entry()
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {vol.Required(CONF_URL, default=entry.data[CONF_URL]): cv.string},
            ),
            description_placeholders={CONF_NAME: entry.title},
        )


def _type_selector(*types: EntityType) -> SelectSelector:
    return SelectSelector(
        SelectSelectorConfig(
            options=[entity_type.value for entity_type in types],
            translation_key="entity_type",
            mode=SelectSelectorMode.DROPDOWN,
        )
    )


class LunatoneOptionsFlow(OptionsFlow):
    """Choose the entity type of each DALI device and group of one gateway.

    DALI device ids and addresses differ between gateways and can change after
    a new scan, so the choice is made per config entry from the live device
    list. Field labels show line, short address, name and gateway id.
    """

    def __init__(self) -> None:
        """Initialize the options flow."""
        self._device_types: dict[str, str] = {}
        self._group_types: dict[str, str] = {}
        self._colors: dict[str, list[int]] = {}

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Choose the type of every DALI device."""
        if self.config_entry.state is not ConfigEntryState.LOADED:
            return self.async_abort(reason="not_loaded")
        coordinator = self.config_entry.runtime_data.coordinator
        devices = sorted(
            coordinator.data.values(), key=lambda d: (d.data.line, d.data.address)
        )
        labels = {
            f"L{d.data.line} A{d.data.address:02d} · {d.name} · "
            f"DT{'/'.join(str(int(t)) for t in d.data.dali_types) or '?'} · id {d.id}": d.id
            for d in devices
        }
        if user_input is not None:
            self._device_types = {
                str(labels[label]): value for label, value in user_input.items() if label in labels
            }
            return await self.async_step_groups()

        selector = _type_selector(
            EntityType.LIGHT, EntityType.COVER, EntityType.SWITCH, EntityType.DISABLED
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(label, default=coordinator.device_type(device_id).value): selector
                    for label, device_id in labels.items()
                }
            ),
        )

    async def async_step_groups(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Choose the type of every DALI group in use."""
        coordinator = self.config_entry.runtime_data.coordinator
        groups = coordinator.groups()
        labels = {
            f"L{line} G{number} · "
            + ", ".join(f"A{coordinator.data[i].data.address}" for i in members): (line, number)
            for (line, number), members in sorted(groups.items())
        }
        if user_input is not None:
            self._group_types = {
                f"{labels[label][0]}-{labels[label][1]}": value
                for label, value in user_input.items()
                if label in labels
            }
            return await self.async_step_colors()

        selector = _type_selector(
            EntityType.LIGHT, EntityType.COVER, EntityType.SWITCH, EntityType.DISABLED
        )
        return self.async_show_form(
            step_id="groups",
            data_schema=vol.Schema(
                {
                    vol.Required(label, default=coordinator.group_type(key).value): selector
                    for label, key in labels.items()
                }
            ),
        )

    async def async_step_colors(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Set the normal colour of colour lights and groups (R, G, B, W as DALI levels 0-254)."""
        coordinator = self.config_entry.runtime_data.coordinator
        current = self.config_entry.options.get(CONF_DEFAULT_COLORS, {})

        def chosen(types: dict[str, str], key: str, fallback: EntityType) -> str:
            return types.get(key, fallback.value)

        targets: dict[str, str] = {}
        for device_id, device in sorted(coordinator.data.items()):
            if device.data.features.color_rgb is None:
                continue
            if chosen(self._device_types, str(device_id), coordinator.device_type(device_id)) != (
                EntityType.LIGHT
            ):
                continue
            label = (
                f"L{device.data.line} A{device.data.address:02d} · {device.name} · id {device_id}"
            )
            targets[label] = device_color_key(device_id)
        for key, members in sorted(coordinator.groups().items()):
            if chosen(self._group_types, group_option_key(key), coordinator.group_type(key)) != (
                EntityType.LIGHT
            ):
                continue
            if not any(coordinator.data[i].data.features.color_rgb for i in members):
                continue
            targets[f"L{key[0]} G{key[1]}"] = group_color_key(key)

        errors: dict[str, str] = {}
        if user_input is not None or not targets:
            colors: dict[str, list[int]] = {}
            for label, value in (user_input or {}).items():
                if label not in targets or not str(value).strip():
                    continue
                if (color := _parse_color(str(value))) is None:
                    errors[label] = "invalid_color"
                else:
                    colors[targets[label]] = color
            if not errors:
                self._colors = colors
                return await self.async_step_covers()

        return self.async_show_form(
            step_id="colors",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        label,
                        description={
                            "suggested_value": ",".join(map(str, current[key]))
                            if key in current
                            else None
                        },
                    ): TextSelector()
                    for label, key in targets.items()
                }
            ),
            errors=errors,
        )


    async def async_step_covers(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Mark the Jalousie modules that are wired the other way round (UP closes)."""
        coordinator = self.config_entry.runtime_data.coordinator
        current = set(self.config_entry.options.get(CONF_INVERTED_COVERS, []))
        covers = {
            f"L{device.data.line} A{device.data.address:02d} · {device.name} · id {device_id}": (
                device_id
            )
            for device_id, device in sorted(coordinator.data.items())
            if self._device_types.get(str(device_id), coordinator.device_type(device_id).value)
            == EntityType.COVER
        }
        if user_input is not None or not covers:
            return self.async_create_entry(
                data={
                    **self.config_entry.options,
                    CONF_DEVICE_TYPES: self._device_types,
                    CONF_GROUP_TYPES: self._group_types,
                    CONF_DEFAULT_COLORS: self._colors,
                    CONF_INVERTED_COVERS: sorted(
                        str(covers[label])
                        for label, value in (user_input or {}).items()
                        if label in covers and value
                    ),
                }
            )
        return self.async_show_form(
            step_id="covers",
            data_schema=vol.Schema(
                {
                    vol.Optional(label, default=str(device_id) in current): BooleanSelector()
                    for label, device_id in covers.items()
                }
            ),
        )


def _parse_color(value: str) -> list[int] | None:
    """Parse 'R,G,B,W' as DALI levels 0-254."""
    try:
        color = [int(part) for part in value.replace(";", ",").split(",")]
    except ValueError:
        return None
    if len(color) != 4 or not all(0 <= part <= 254 for part in color):
        return None
    return color
