"""Tests for DALI frame decoding."""

from custom_components.lunatone.dali import (
    Opcode,
    TargetKind,
    level_to_percent,
    parse_monitor_event,
)


def _frame(data: list[int], bits: int = 16, external: bool = True) -> dict:
    return {"bits": bits, "data": data, "line": 0, "externalSource": external}


def test_group_command() -> None:
    """[137, 5] is RECALL MAX to group 4 (captured from the living room switch)."""
    frame = parse_monitor_event(_frame([137, 5]))
    assert frame is not None
    assert frame.kind is TargetKind.GROUP
    assert frame.address == 4
    assert frame.opcode == Opcode.RECALL_MAX_LEVEL
    assert frame.level is None
    assert frame.targets(0, 2, [4])
    assert not frame.targets(0, 2, [5])
    assert not frame.targets(1, 2, [4])


def test_short_address_commands() -> None:
    """Jalousie at address 11: UP [23, 1], OFF [23, 0], DOWN [23, 2]."""
    up = parse_monitor_event(_frame([23, 1]))
    assert up is not None
    assert (up.kind, up.address, up.opcode) == (TargetKind.SHORT, 11, Opcode.UP)
    assert up.targets(0, 11, [])
    assert not up.targets(0, 12, [])
    off = parse_monitor_event(_frame([23, 0]))
    assert off is not None and off.opcode == Opcode.OFF


def test_direct_arc_power() -> None:
    """DAPC: even address byte, second byte is the level."""
    frame = parse_monitor_event(_frame([0x9E, 0xFE]))
    assert frame is not None
    assert frame.kind is TargetKind.GROUP
    assert frame.address == 15
    assert frame.level == 254
    assert frame.opcode is None


def test_broadcast_scene_and_query() -> None:
    """Broadcast, GO TO SCENE and queries."""
    broadcast = parse_monitor_event(_frame([0xFF, 0x00]))
    assert broadcast is not None
    assert broadcast.kind is TargetKind.BROADCAST
    assert broadcast.targets(0, 30, [])
    scene = parse_monitor_event(_frame([0x9F, 0x13]))
    assert scene is not None and scene.scene == 3
    query = parse_monitor_event(_frame([5, 0xA0]))
    assert query is not None and query.is_query


def test_ignored_frames() -> None:
    """Answers, 24 bit frames and special commands are not decoded."""
    assert parse_monitor_event(_frame([255], bits=8)) is None
    assert parse_monitor_event(_frame([1, 2, 3], bits=24)) is None
    assert parse_monitor_event(_frame([0xA3, 0x10])) is None  # DTR0
    assert parse_monitor_event({"bits": 16, "data": "x"}) is None


def test_level_to_percent() -> None:
    """Same scale as the gateway (1 -> 0.3937 %, 254 -> 100 %)."""
    assert level_to_percent(254) == 100
    assert round(level_to_percent(1), 4) == 0.3937
