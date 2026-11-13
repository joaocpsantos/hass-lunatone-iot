"""DALI forward frames sent to the gateway as WebSocket `daliFrame` messages.

Commands go over the WebSocket that is already open instead of REST. The
gateway decodes them like any other bus traffic and reports the resulting
state in `devices` events (measured: level ~140 ms, DT8 colour ~270 ms).
The DT8 colour sequences are the ones the gateway itself emits for its REST
colour commands, except that RGB and WAF are both loaded before a single
DAPC, so colour and level change together without an intermediate state.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from .dali import MAX_LEVEL, Opcode, short_address_byte

DTR0 = 0xA3
DTR1 = 0xC3
DTR2 = 0xC5
ENABLE_DEVICE_TYPE = 0xC1
DT8 = 8
# IEC 62386-209 application extended commands (preceded by ENABLE DEVICE TYPE 8)
SET_TEMPORARY_COLOUR_TEMPERATURE = 0xE7
SET_TEMPORARY_RGB_DIMLEVEL = 0xEB
SET_TEMPORARY_WAF_DIMLEVEL = 0xEC
ACTIVATE = 0xE2

type Frame = list[int]


@dataclass(frozen=True, slots=True)
class Target:
    """Destination of a command: a short address, a group or the broadcast."""

    line: int
    address_byte: int  # with the selector bit cleared (direct arc power)

    @classmethod
    def device(cls, line: int, address: int) -> Target:
        """Return a short address target."""
        return cls(line, short_address_byte(address, command=False))

    @classmethod
    def group(cls, line: int, group: int) -> Target:
        """Return a group target."""
        return cls(line, 0x80 | (group << 1))

    @classmethod
    def broadcast(cls, line: int) -> Target:
        """Return the broadcast target."""
        return cls(line, 0xFE)

    def dapc(self, level: int) -> Frame:
        """Direct arc power control."""
        return [self.address_byte, level]

    def command(self, opcode: int) -> Frame:
        """Indirect command (selector bit set)."""
        return [self.address_byte | 1, opcode]


def percent_to_level(percent: float) -> int:
    """Convert the gateway's 0-100 % scale to a DALI arc power level (0-254)."""
    level = round(percent / 100 * MAX_LEVEL)
    if percent > 0 and level == 0:
        return 1
    return max(0, min(MAX_LEVEL, level))


def channel(value: float) -> int:
    """Convert a 0-1 colour channel to a DT8 dim level (0-254)."""
    return max(0, min(MAX_LEVEL, round(value * MAX_LEVEL)))


def _dt8(target: Target, dtr: Sequence[int], opcode: int) -> list[Frame]:
    frames = [[register, value] for register, value in zip((DTR0, DTR1, DTR2), dtr, strict=False)]
    frames += [[ENABLE_DEVICE_TYPE, DT8], target.command(opcode)]
    return frames


def level_frames(target: Target, percent: float) -> list[Frame]:
    """Set the level (0 % switches off with the configured fade)."""
    return [target.dapc(percent_to_level(percent))]


def color_frames(
    target: Target,
    percent: float,
    rgb: tuple[float, float, float] | None = None,
    white: float | None = None,
    kelvin: int | None = None,
) -> list[Frame]:
    """Load the temporary colour and apply it together with the level."""
    frames: list[Frame] = []
    if rgb is not None:
        frames += _dt8(target, [channel(value) for value in rgb], SET_TEMPORARY_RGB_DIMLEVEL)
    if white is not None:
        frames += _dt8(target, [channel(white), 0, 0], SET_TEMPORARY_WAF_DIMLEVEL)
    if kelvin is not None:
        mirek = max(1, min(0xFFFE, round(1_000_000 / kelvin)))
        frames += _dt8(target, [mirek & 0xFF, mirek >> 8], SET_TEMPORARY_COLOUR_TEMPERATURE)
    if percent > 0:
        # DAPC activates the temporary colour together with the new level.
        frames.append(target.dapc(percent_to_level(percent)))
    elif frames:
        frames += [[ENABLE_DEVICE_TYPE, DT8], target.command(ACTIVATE)]
    return frames


def on_frames(target: Target) -> list[Frame]:
    """RECALL MAX LEVEL (switching actuators, Jalousie up)."""
    return [target.command(Opcode.RECALL_MAX_LEVEL)]


def off_frames(target: Target) -> list[Frame]:
    """OFF (immediate; Jalousie stop)."""
    return [target.command(Opcode.OFF)]


def up_frames(target: Target) -> list[Frame]:
    """UP (Jalousie: move up until OFF)."""
    return [target.command(Opcode.UP)]


def down_frames(target: Target) -> list[Frame]:
    """DOWN (Jalousie: move down until OFF)."""
    return [target.command(Opcode.DOWN)]
