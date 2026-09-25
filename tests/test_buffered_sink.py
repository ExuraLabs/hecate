"""BufferedSink: what reaches downstream, and what a rollback does to it.

The buffer is the reason a one- or two-block rollback — the common kind at the
tip — never reaches a consumer at all. These pin both halves of that: shallow
rollbacks are absorbed without a trace, deeper ones are handed on exactly once,
and anything that does not fit the chain is refused before the buffer moves.
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest
from ogmios import Block, Point

from errors import ChainLinkError
from sinks.base import BufferedSink
from tests.fake_ogmios import ANCHOR, FakeBlock, block_hash, grow

MAIN = grow(ANCHOR, 8)


class Recorder:
    """A downstream that remembers everything it was sent, in order."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, dict[str, Any]]] = []

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        self.calls.append(("batch", [block.id for block in blocks], kwargs))

    async def send_rollback(self, point: Point, **kwargs: Any) -> None:
        self.calls.append(("rollback", (point.slot, point.id), kwargs))

    @property
    def relayed(self) -> list[str]:
        return [
            block_id
            for kind, ids, _ in self.calls
            if kind == "batch"
            for block_id in ids
        ]


def run(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(scenario())


def buffered(recorder: Recorder, depth: int = 2) -> BufferedSink:
    return BufferedSink(recorder, base=ANCHOR.point, depth=depth)


def blocks(chain: list[FakeBlock]) -> list[Block]:
    return [block.as_block() for block in chain]


def test_a_block_is_relayed_once_depth_blocks_follow_it() -> None:
    recorder = Recorder()
    sink = buffered(recorder)

    async def scenario() -> None:
        await sink.send_batch(blocks(MAIN[:2]))
        assert recorder.calls == []

        await sink.send_block(MAIN[2].as_block())
        assert recorder.relayed == [MAIN[0].hash]
        assert sink.base == MAIN[0].point
        assert [held.block.id for held in sink.held] == [MAIN[1].hash, MAIN[2].hash]

    run(scenario)


def test_each_block_is_relayed_with_the_context_it_arrived_with() -> None:
    """A block relayed later still carries its own tip, not the latest one."""
    recorder = Recorder()
    sink = buffered(recorder)

    async def scenario() -> None:
        for index, block in enumerate(MAIN[:5]):
            await sink.send_block(block.as_block(), tip=index)

    run(scenario)

    assert recorder.calls == [
        ("batch", [MAIN[0].hash], {"tip": 0}),
        ("batch", [MAIN[1].hash], {"tip": 1}),
        ("batch", [MAIN[2].hash], {"tip": 2}),
    ]


def test_blocks_sharing_a_context_go_downstream_together() -> None:
    recorder = Recorder()
    sink = buffered(recorder)

    run(lambda: sink.send_batch(blocks(MAIN[:6]), tip="same"))

    assert recorder.calls == [
        ("batch", [block.hash for block in MAIN[:4]], {"tip": "same"})
    ]


def test_depth_zero_relays_immediately() -> None:
    recorder = Recorder()
    sink = buffered(recorder, depth=0)

    run(lambda: sink.send_block(MAIN[0].as_block()))

    assert recorder.relayed == [MAIN[0].hash]
    assert not sink.held


def test_a_rollback_inside_the_buffer_drops_the_newest_blocks_silently() -> None:
    recorder = Recorder()
    sink = buffered(recorder)
    fork = grow(MAIN[3], 3, fork="fork")

    async def scenario() -> None:
        await sink.send_batch(blocks(MAIN[:6]))  # relays 0-3, holds 4-5
        await sink.rollback_to(MAIN[4].point)
        assert [held.block.id for held in sink.held] == [MAIN[4].hash]

        await sink.rollback_to(MAIN[3].point)  # the base itself: holds nothing
        assert not sink.held

        await sink.send_batch(blocks(fork))

    run(scenario)

    assert all(kind == "batch" for kind, _, _ in recorder.calls)
    assert recorder.relayed == [block.hash for block in MAIN[:4] + fork[:1]]


def test_a_rollback_below_the_buffer_is_handed_downstream() -> None:
    recorder = Recorder()
    sink = buffered(recorder)
    fork = grow(MAIN[1], 3, fork="fork")

    async def scenario() -> None:
        await sink.send_batch(blocks(MAIN[:6]))  # relays 0-3, holds 4-5
        await sink.rollback_to(MAIN[1].point, tip="rolled")
        assert not sink.held
        assert sink.base == MAIN[1].point

        await sink.send_batch(blocks(fork))

    run(scenario)

    assert recorder.calls[1] == (
        "rollback",
        (MAIN[1].slot, MAIN[1].hash),
        {"tip": "rolled"},
    )
    assert recorder.relayed == [block.hash for block in MAIN[:4] + fork[:1]]


def test_a_block_that_does_not_extend_the_chain_is_refused() -> None:
    recorder = Recorder()
    sink = buffered(recorder)
    stray = FakeBlock(
        slot=MAIN[2].slot, hash=block_hash("stray"), height=0, ancestor=MAIN[0].hash
    )

    async def scenario() -> None:
        await sink.send_batch(blocks(MAIN[:2]))
        with pytest.raises(ChainLinkError) as raised:
            await sink.send_batch([MAIN[2].as_block(), stray.as_block()])
        assert raised.value.exit_code == 18

    run(scenario)

    assert recorder.calls == []
    assert [held.block.id for held in sink.held] == [MAIN[0].hash, MAIN[1].hash]


def test_the_first_block_must_extend_the_starting_point() -> None:
    sink = buffered(Recorder())

    with pytest.raises(ChainLinkError):
        run(lambda: sink.send_block(MAIN[1].as_block()))


@pytest.mark.parametrize(
    "point",
    [
        # A buffered block's slot, someone else's block.
        Point(slot=MAIN[4].slot, id=block_hash("competitor")),
        # The base's slot, someone else's block.
        Point(slot=MAIN[3].slot, id=block_hash("competitor")),
        # Between two held blocks, matching neither.
        Point(slot=MAIN[4].slot + 1, id=block_hash("between")),
        # Past everything this buffer has seen.
        Point(slot=MAIN[5].slot + 20, id=block_hash("future")),
    ],
    ids=["held-slot", "base-slot", "between", "ahead"],
)
def test_a_rollback_to_a_point_off_the_chain_is_refused(point: Point) -> None:
    recorder = Recorder()
    sink = buffered(recorder)

    async def scenario() -> None:
        await sink.send_batch(blocks(MAIN[:6]))
        with pytest.raises(ChainLinkError):
            await sink.rollback_to(point)

    run(scenario)

    assert [kind for kind, _, _ in recorder.calls] == ["batch"]
    assert [held.block.id for held in sink.held] == [MAIN[4].hash, MAIN[5].hash]
