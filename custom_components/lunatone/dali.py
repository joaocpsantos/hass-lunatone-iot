"""Decoding of DALI forward frames reported by the gateway's daliMonitor events."""

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any

MASK = 255
QUERY_ACTUAL_LEVEL = 0xA0
# DT8 (IEC 62386-209), after DTR0 = selector and ENABLE DEVICE TYPE 8
QUERY_COLOUR_VALUE = 0xFA
MAX_LEVEL = 254


class Opcode(IntEnum):
    """IEC 62386-102 indirect arc power commands we care about."""

    OFF = 0x00
    UP = 0x01
    DOWN = 0x02
    STEP_UP = 0x03
    STEP_DOWN = 0x04
    RECALL_MAX_LEVEL = 0x05
    RECALL_MIN_LEVEL = 0x06
    STEP_DOWN_AND_OFF = 0x07
    ON_AND_STEP_UP = 0x08
    GO_TO_LAST_ACTIVE_LEVEL = 0x0A


class TargetKind(StrEnum):
    """Addressing mode of a forward frame."""

    SHORT = "short"
    GROUP = "group"
    BROADCAST = "broadcast"
    BROADCAST_UNADDRESSED = "broadcast_unaddressed"


# Commands that change the light level without the gateway tracking the result.
LEVEL_UNTRACKED_OPCODES = frozenset(
    {
        Opcode.UP,
        Opcode.DOWN,
        Opcode.STEP_UP,
        Opcode.STEP_DOWN,
        Opcode.ON_AND_STEP_UP,
        Opcode.STEP_DOWN_AND_OFF,
        Opcode.GO_TO_LAST_ACTIVE_LEVEL,
    }
)


@dataclass(frozen=True, slots=True)
class ForwardFrame:
    """A decoded 16 bit DALI forward frame."""

    line: int
    kind: TargetKind
    address: int | None
    # Direct arc power level (DAPC) or None for an indirect command.
    level: int | None
    opcode: int | None
    external: bool

    @property
    def scene(self) -> int | None:
        """Return the scene number for GO TO SCENE commands."""
        if self.opcode is not None and 0x10 <= self.opcode <= 0x1F:
            return self.opcode - 0x10
        return None

    @property
    def is_query(self) -> bool:
        """Return True for query commands (no effect on the gear)."""
        return self.opcode is not None and self.opcode >= 0x90

    def targets(self, line: int, address: int, groups: list[int]) -> bool:
        """Return True if the gear at line/address with these groups receives it."""
        if line != self.line:
            return False
        if self.kind is TargetKind.SHORT:
            return self.address == address
        if self.kind is TargetKind.GROUP:
            return self.address in groups
        return self.kind is TargetKind.BROADCAST


def short_address_byte(address: int, command: bool = True) -> int:
    """Return the address byte of a frame sent to a short address."""
    return (address << 1) | int(command)


def parse_monitor_event(data: dict[str, Any]) -> ForwardFrame | None:
    """Decode the data field of a daliMonitor event, or None if not relevant."""
    frame = data.get("data")
    if data.get("bits") != 16 or not isinstance(frame, list) or len(frame) != 2:
        return None
    addr, value = frame
    if not isinstance(addr, int) or not isinstance(value, int):
        return None

    address: int | None
    if addr & 0x80 == 0:
        kind, address = TargetKind.SHORT, (addr >> 1) & 0x3F
    elif addr & 0xE0 == 0x80:
        kind, address = TargetKind.GROUP, (addr >> 1) & 0x0F
    elif addr in (0xFE, 0xFF):
        kind, address = TargetKind.BROADCAST, None
    elif addr in (0xFC, 0xFD):
        kind, address = TargetKind.BROADCAST_UNADDRESSED, None
    else:
        # Special commands (addressing, DTR, ...)
        return None

    direct = addr & 1 == 0
    return ForwardFrame(
        line=int(data.get("line", 0)),
        kind=kind,
        address=address,
        level=value if direct else None,
        opcode=None if direct else value,
        external=bool(data.get("externalSource", True)),
    )


def level_to_percent(level: int) -> float:
    """Convert a DALI arc power level (0-254) to the gateway's percentage."""
    return level / MAX_LEVEL * 100
