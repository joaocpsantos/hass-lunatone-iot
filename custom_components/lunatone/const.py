"""Constants for the Lunatone integration."""

from enum import StrEnum
from typing import Final

DOMAIN: Final = "lunatone"
MANUFACTURER: Final = "Lunatone Industrielle Elektronik GmbH"

CONF_DEVICE_TYPES: Final = "device_types"
CONF_GROUP_TYPES: Final = "group_types"
# Colour (R, G, B, W as DALI levels 0-254) sent when a colour light is turned on without a colour.
CONF_DEFAULT_COLORS: Final = "default_colors"
# Jalousie modules wired the other way round (UP closes): device ids as strings.
CONF_INVERTED_COVERS: Final = "inverted_covers"


class EntityType(StrEnum):
    """Home Assistant entity type chosen for a DALI device or group."""

    LIGHT = "light"
    COVER = "cover"
    SWITCH = "switch"
    DISABLED = "disabled"


class CoverMotion(StrEnum):
    """Motion of a Lunatone DALI Jalousie, as seen on the DALI bus."""

    OPENING = "opening"
    CLOSING = "closing"
    STOPPED = "stopped"
