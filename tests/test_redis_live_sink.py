"""RedisLiveSink against a real Redis: the single writer, trimming and backpressure.

Everything the live stream promises consumers rests on Lua scripts checking
and writing in one step, so these run against a Redis rather than a fake: the
properties under test are the atomicity of those scripts and what Redis does
with ``XTRIM MINID``, not anything in Python.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, cast

import orjson
import pytest
from ogmios import Tip

from errors import LeaseLostError, TailMovedError
from tests.fake_ogmios import ANCHOR, FakeBlock, grow

pytest.importorskip("redis", reason="the live sink needs the redis group")

from sinks.redis_live import LivePolicy, RedisLiveSink  # noqa: E402

if TYPE_CHECKING:
    import redis

NS = "hecate:test-live"
STREAM, SLOTS, STATE, PRODUCER, CONSUMERS = (
    f"{NS}:{part}" for part in ("stream", "slots", "state", "producer", "consumers")
)

#: Everything a tenth of a second or so, so lapses and pauses are observable.
QUICK = LivePolicy(
    lease_seconds=0.3,
    heartbeat_seconds=0.05,
    retain_blocks=10,
    max_retained_blocks=20,
    max_unconsumed_blocks=10,
    trim_interval_seconds=0.05,
    backpressure_check_seconds=0.02,
)

CHAIN = grow(ANCHOR, 30)

Entries = list[tuple[str, dict[str, str]]]


def run(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(scenario())


def live(policy: LivePolicy = QUICK) -> RedisLiveSink:
    return RedisLiveSink(namespace=NS, policy=policy)


async def take_over(sink: RedisLiveSink, base: FakeBlock = ANCHOR) -> None:
    """Hold the lease and start an empty stream right after ``base``."""
    assert await sink.try_acquire_lease()
    await sink.anchor_empty_stream(base.point)


async def publish(sink: RedisLiveSink, blocks: list[FakeBlock]) -> None:
    await sink.send_batch([block.as_block() for block in blocks], tip=blocks[-1].tip)


def entries(conn: redis.Redis) -> Entries:
    return cast(Entries, conn.xrange(STREAM))


def block_hashes(conn: redis.Redis) -> list[str]:
    return [fields["hash"] for _, fields in entries(conn) if fields["type"] == "block"]


def entry_id_of(conn: redis.Redis, block: FakeBlock) -> str:
    (entry_id,) = [
        entry_id
        for entry_id, fields in entries(conn)
        if fields["type"] == "block" and fields["hash"] == block.hash
    ]
    return entry_id


def register(
    conn: redis.Redis,
    block: FakeBlock,
    *,
    entry_id: str | None,
    age_seconds: float = 0.0,
    group: str = "reader",
) -> None:
    """Stand in for a consumer writing its anchor after a commit."""
    conn.hset(
        CONSUMERS,
        group,
        orjson.dumps(
            {
                "slot": block.slot,
                "hash": block.hash,
                "entry_id": entry_id,
                "updated_at": time.time() - age_seconds,
            }
        ),
    )


def assert_index_matches_stream(conn: redis.Redis) -> None:
    """One ``{hash}:{entry_id}`` member per block entry, scored by its slot."""
    expected = {
        f"{fields['hash']}:{entry_id}": float(fields["slot"])
        for entry_id, fields in entries(conn)
        if fields["type"] == "block"
    }
    members = dict(
        cast(list[tuple[str, float]], conn.zrange(SLOTS, 0, -1, withscores=True))
    )
    assert members == expected


# -- the single writer --------------------------------------------------------


def test_only_one_follower_holds_the_lease() -> None:
    async def scenario() -> None:
        async with live() as first, live() as second:
            assert await first.try_acquire_lease()
            assert not await second.try_acquire_lease()
            assert await second.lease_holder() == first.producer_id

    run(scenario)


def test_a_standby_takes_over_a_lapsed_lease_and_fences_the_old_writer_out(
    conn: redis.Redis,
) -> None:
    """The old writer is not told it lost the lease; its next write finds out.

    That is the case the fencing exists for: a producer stalled past its lease
    resumes believing it is still the writer.
    """

    async def scenario() -> None:
        async with live() as old, live() as new:
            await take_over(old)
            await publish(old, CHAIN[:1])

            await asyncio.sleep(QUICK.lease_seconds * 1.5)
            assert await new.try_acquire_lease()

            with pytest.raises(LeaseLostError) as raised:
                await publish(old, CHAIN[1:2])
            assert raised.value.holder == new.producer_id
            assert raised.value.exit_code == 16
            with pytest.raises(LeaseLostError):
                await old.heartbeat()

            new.adopt_tail(CHAIN[0].point)
            await publish(new, CHAIN[1:2])

    run(scenario)

    assert block_hashes(conn) == [CHAIN[0].hash, CHAIN[1].hash]


def test_the_heartbeat_keeps_the_lease_past_its_lifetime(conn: redis.Redis) -> None:
    async def scenario() -> None:
        async with live() as writer, live() as standby:
            assert await writer.try_acquire_lease()
            upkeep = asyncio.create_task(writer.run_upkeep())
            await asyncio.sleep(QUICK.lease_seconds * 3)
            assert not await standby.try_acquire_lease()
            upkeep.cancel()

    run(scenario)

    state = cast(dict[str, str], conn.hgetall(STATE))
    assert state["paused"] == "0"
    assert float(state["heartbeat_ts"]) > time.time() - 1


def test_releasing_the_lease_only_drops_our_own(conn: redis.Redis) -> None:
    async def scenario() -> None:
        async with live() as holder, live() as other:
            assert await holder.try_acquire_lease()
            await other.release_lease()
            assert await other.lease_holder() == holder.producer_id
            await holder.release_lease()
            assert await other.lease_holder() is None

    run(scenario)


# -- the order invariant ------------------------------------------------------


def test_a_tip_reported_at_height_zero_is_published_without_one(
    conn: redis.Redis,
) -> None:
    """Ogmios in front of dolos reports every tip at height 0."""
    unmeasured = Tip(slot=CHAIN[1].slot, id=CHAIN[1].hash, height=0)

    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await sink.send_batch(
                [block.as_block() for block in CHAIN[:2]], tip=unmeasured
            )

    run(scenario)

    tips = [(fields["tip_slot"], fields["tip_height"]) for _, fields in entries(conn)]
    assert tips == [(str(CHAIN[1].slot), "")] * 2
    assert cast(dict[str, str], conn.hgetall(STATE))["tip_height"] == ""


def test_a_block_entry_carries_the_contract_fields(conn: redis.Redis) -> None:
    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, CHAIN[:2])

    run(scenario)

    (first_id, first), (_, second) = entries(conn)
    assert first["type"] == "block"
    assert (first["slot"], first["hash"], first["height"], first["ancestor"]) == (
        str(CHAIN[0].slot),
        CHAIN[0].hash,
        str(CHAIN[0].height),
        ANCHOR.hash,
    )
    assert (first["tip_slot"], first["tip_height"]) == (
        str(CHAIN[1].slot),
        str(CHAIN[1].height),
    )
    payload = orjson.loads(first["data"])
    assert isinstance(payload, dict), "one block per entry, not a batch"
    assert (payload["hash"], payload["slot"]) == (CHAIN[0].hash, CHAIN[0].slot)
    assert "datums" not in payload["transactions"][0]
    assert second["ancestor"] == first["hash"]

    state = cast(dict[str, str], conn.hgetall(STATE))
    assert (state["last_slot"], state["last_hash"], state["last_height"]) == (
        str(CHAIN[1].slot),
        CHAIN[1].hash,
        str(CHAIN[1].height),
    )
    assert conn.zscore(SLOTS, f"{CHAIN[0].hash}:{first_id}") == CHAIN[0].slot


def test_a_block_that_does_not_extend_the_tail_writes_nothing(
    conn: redis.Redis,
) -> None:
    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            with pytest.raises(TailMovedError):
                await publish(sink, CHAIN[1:3])

    run(scenario)

    assert entries(conn) == []
    assert conn.zcard(SLOTS) == 0


def test_a_tail_moved_by_another_writer_fences_this_one_out(
    conn: redis.Redis,
) -> None:
    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, CHAIN[:1])
            conn.hset(STATE, "last_hash", "f" * 64)
            with pytest.raises(TailMovedError) as raised:
                await publish(sink, CHAIN[1:2])
            assert raised.value.exit_code == 16
            with pytest.raises(TailMovedError):
                await sink.send_rollback(ANCHOR.point, tip=CHAIN[0].tip)

    run(scenario)

    assert block_hashes(conn) == [CHAIN[0].hash]


def test_a_rollback_entry_moves_the_tail_back_to_its_point(
    conn: redis.Redis,
) -> None:
    fork = grow(CHAIN[0], 2, fork="fork")

    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, CHAIN[:3])
            with pytest.raises(TailMovedError):
                await sink.send_rollback(CHAIN[2].point, tip=CHAIN[2].tip)
            await sink.send_rollback(CHAIN[0].point, tip=fork[-1].tip)
            await publish(sink, fork)

    run(scenario)

    kinds = [(fields["type"], fields["hash"]) for _, fields in entries(conn)]
    assert kinds == [
        ("block", CHAIN[0].hash),
        ("block", CHAIN[1].hash),
        ("block", CHAIN[2].hash),
        ("rollback", CHAIN[0].hash),
        ("block", fork[0].hash),
        ("block", fork[1].hash),
    ]
    _, rollback = entries(conn)[3]
    assert set(rollback) == {"type", "slot", "hash", "tip_slot", "tip_height"}
    assert rollback["tip_slot"] == str(fork[-1].slot)


def test_the_canonical_walk_skips_what_rollbacks_orphaned(conn: redis.Redis) -> None:
    main = CHAIN[:10]
    fork = grow(main[4], 3, fork="fork")

    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, main)
            await sink.send_rollback(main[4].point, tip=fork[-1].tip)
            await publish(sink, fork)

            points = await sink.canonical_points()
            assert [point.hash for point in points] == [
                block.hash for block in reversed(main[:5] + fork)
            ]
            assert all(point.height is not None for point in points)
            assert len(await sink.canonical_points(limit=3)) == 3

    run(scenario)


def test_anchoring_a_stream_that_already_has_entries_is_refused() -> None:
    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, CHAIN[:1])
            with pytest.raises(TailMovedError):
                await sink.anchor_empty_stream(ANCHOR.point)

    run(scenario)


# -- retention ----------------------------------------------------------------


def published(conn: redis.Redis, blocks: list[FakeBlock] = CHAIN) -> None:
    """A stream a follower filled with ``blocks`` and then stopped."""

    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, blocks)
            await sink.release_lease()

    run(scenario)


def trimmed() -> tuple[int, int]:
    result: tuple[int, int] = (0, 0)

    async def scenario() -> None:
        nonlocal result
        async with live() as sink:
            assert await sink.try_acquire_lease()
            result = await sink.trim()

    run(scenario)
    return result


def test_trimming_keeps_retain_blocks_and_the_index_with_them(
    conn: redis.Redis,
) -> None:
    published(conn)
    register(conn, CHAIN[-1], entry_id=entry_id_of(conn, CHAIN[-1]))

    assert trimmed() == (20, 20)

    assert block_hashes(conn) == [block.hash for block in CHAIN[20:]]
    assert_index_matches_stream(conn)


def test_trimming_never_passes_an_active_consumer(conn: redis.Redis) -> None:
    published(conn)
    register(conn, CHAIN[4], entry_id=entry_id_of(conn, CHAIN[4]))

    trimmed()

    assert block_hashes(conn) == [block.hash for block in CHAIN[4:]]
    assert_index_matches_stream(conn)


def test_a_stream_no_consumer_has_registered_on_is_never_trimmed(
    conn: redis.Redis,
) -> None:
    published(conn)

    assert trimmed() == (0, 0)
    assert len(block_hashes(conn)) == len(CHAIN)
    assert_index_matches_stream(conn)


@pytest.mark.parametrize("entry_id", [None, ""])
def test_a_consumer_anchored_before_every_entry_pins_everything(
    conn: redis.Redis, entry_id: str | None
) -> None:
    published(conn)
    register(conn, ANCHOR, entry_id=entry_id)

    assert trimmed() == (0, 0)
    assert len(block_hashes(conn)) == len(CHAIN)


@pytest.mark.parametrize("past_the_block", [0, 5], ids=["at-a-block", "between-blocks"])
def test_a_consumer_that_has_read_nothing_pins_the_block_at_or_before_its_slot(
    conn: redis.Redis, past_the_block: int
) -> None:
    """The block a slot-only position anchors at, as Kupo places it."""
    published(conn)
    register(
        conn, replace(CHAIN[4], slot=CHAIN[4].slot + past_the_block), entry_id=None
    )

    trimmed()

    assert block_hashes(conn) == [block.hash for block in CHAIN[4:]]
    assert_index_matches_stream(conn)


def test_a_consumer_waiting_ahead_of_the_stream_pins_nothing_behind_it(
    conn: redis.Redis,
) -> None:
    """One workload anchored near the tip while another catches up from far
    behind: the stream is owed to the slower one, not held whole for both."""
    published(conn)
    register(conn, CHAIN[4], entry_id=entry_id_of(conn, CHAIN[4]), group="behind")
    register(conn, grow(CHAIN[-1], 5)[-1], entry_id=None, group="ahead")

    trimmed()

    assert block_hashes(conn) == [block.hash for block in CHAIN[4:]]
    assert_index_matches_stream(conn)


def test_a_stale_consumer_pins_at_most_max_retained_blocks(
    conn: redis.Redis,
) -> None:
    published(conn)
    register(conn, CHAIN[1], entry_id=entry_id_of(conn, CHAIN[1]), age_seconds=3600)

    trimmed()

    assert block_hashes(conn) == [block.hash for block in CHAIN[10:]]
    assert_index_matches_stream(conn)


def test_a_stale_consumer_within_the_cap_still_pins(conn: redis.Redis) -> None:
    published(conn)
    register(conn, CHAIN[14], entry_id=entry_id_of(conn, CHAIN[14]), age_seconds=3600)

    trimmed()

    assert block_hashes(conn) == [block.hash for block in CHAIN[14:]]


def test_trimming_after_a_rollback_keeps_the_index_exact(conn: redis.Redis) -> None:
    """A fork re-publishes lower slots after higher ones, so slot order and
    stream order disagree; the index must still lose exactly what the stream
    does, and the newest ``retain_blocks`` block entries must all survive.
    """
    main = CHAIN[:10]
    fork = grow(main[4], 10, fork="fork", slot_step=7)

    async def scenario() -> None:
        async with live() as sink:
            await take_over(sink)
            await publish(sink, main)
            await sink.send_rollback(main[4].point, tip=fork[-1].tip)
            await publish(sink, fork)
            register(conn, fork[-1], entry_id=entry_id_of(conn, fork[-1]))
            await sink.trim()

    run(scenario)

    remaining = block_hashes(conn)
    assert remaining[-QUICK.retain_blocks :] == [
        block.hash for block in fork[-QUICK.retain_blocks :]
    ]
    assert main[0].hash not in remaining
    assert_index_matches_stream(conn)


# -- backpressure -------------------------------------------------------------


def test_backpressure_pauses_for_a_lagging_consumer_and_keeps_heartbeating(
    conn: redis.Redis,
) -> None:
    published(conn)
    register(conn, CHAIN[4], entry_id=entry_id_of(conn, CHAIN[4]))

    async def scenario() -> None:
        async with live() as sink:
            assert await sink.try_acquire_lease()
            upkeep = asyncio.create_task(sink.run_upkeep())
            waiting = asyncio.create_task(sink.wait_for_backpressure())

            await asyncio.sleep(0.3)
            assert not waiting.done()
            state = cast(dict[str, str], conn.hgetall(STATE))
            assert state["paused"] == "1"
            assert float(state["paused_since"]) <= time.time()
            beat = float(state["heartbeat_ts"])

            await asyncio.sleep(0.2)
            state = cast(dict[str, str], conn.hgetall(STATE))
            assert float(state["heartbeat_ts"]) > beat, "no heartbeat while paused"

            register(conn, CHAIN[25], entry_id=entry_id_of(conn, CHAIN[25]))
            await asyncio.wait_for(waiting, timeout=5)
            upkeep.cancel()

    run(scenario)

    state = cast(dict[str, str], conn.hgetall(STATE))
    assert (state["paused"], state["paused_since"]) == ("0", "")


def test_a_stream_nobody_has_registered_on_waits_for_its_first_consumer(
    conn: redis.Redis,
) -> None:
    published(conn)

    async def scenario() -> None:
        async with live() as sink:
            assert await sink.try_acquire_lease()
            assert await sink.unconsumed_blocks() == len(CHAIN)
            waiting = asyncio.create_task(sink.wait_for_backpressure())
            await asyncio.sleep(0.2)
            assert not waiting.done()

            register(conn, CHAIN[25], entry_id=entry_id_of(conn, CHAIN[25]))
            await asyncio.wait_for(waiting, timeout=5)

    run(scenario)


def test_a_stale_consumer_applies_no_backpressure(conn: redis.Redis) -> None:
    published(conn)
    register(conn, CHAIN[0], entry_id=entry_id_of(conn, CHAIN[0]), age_seconds=3600)

    async def scenario() -> None:
        async with live() as sink:
            assert await sink.unconsumed_blocks() == 0
            await asyncio.wait_for(sink.wait_for_backpressure(), timeout=1)

    run(scenario)


def test_lag_counts_block_entries_past_the_slowest_active_anchor(
    conn: redis.Redis,
) -> None:
    published(conn)
    register(conn, CHAIN[19], entry_id=entry_id_of(conn, CHAIN[19]), group="fast")
    register(conn, CHAIN[9], entry_id=entry_id_of(conn, CHAIN[9]), group="slow")

    async def scenario() -> None:
        async with live() as sink:
            assert await sink.unconsumed_blocks() == 20

    run(scenario)


def test_an_unreadable_consumer_registration_is_an_error(conn: redis.Redis) -> None:
    published(conn)
    conn.hset(CONSUMERS, "broken", "not json")

    async def scenario() -> None:
        async with live() as sink:
            with pytest.raises(ValueError, match="broken"):
                await sink.unconsumed_blocks()

    run(scenario)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"lease_seconds": 0.1, "heartbeat_seconds": 0.05}, "three heartbeats"),
        ({"retain_blocks": 0}, "retain_blocks"),
        ({"retain_blocks": 50, "max_retained_blocks": 40}, "max_retained_blocks"),
    ],
    ids=["lease-too-short", "retain-nothing", "cap-below-retain"],
)
def test_policies_that_cannot_hold_are_refused(
    changes: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        LivePolicy(**changes)
