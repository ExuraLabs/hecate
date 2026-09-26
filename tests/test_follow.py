"""`follow` end to end: a fake Ogmios, the suite's Redis, and the stream between.

The fake node serves a chain the test controls and rolls a follower back the
way a real one would when that chain is swapped for a fork (see
``tests/fake_ogmios.py``). So these exercise the whole path — start point,
chain-sync, the buffer, the fenced writes — and assert on what a consumer
would read: the entries in the stream, and that each one extends the last.

The in-process scenarios stop the follower through its ``stop`` event. The
exit-code tests run the CLI as a subprocess, the way a supervisor does.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import threading
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, cast

import orjson
import pytest
from ogmios import Point

from epoch_derivation import epoch_start_slot
from errors import LeaseLostError, StartPointRefusedError
from models import EpochNumber
from tests.conftest import REPO_ROOT
from tests.fake_ogmios import ANCHOR, FakeBlock, FakeOgmios, block_hash, grow

pytest.importorskip("redis", reason="follow writes to Redis")

import follow as live  # noqa: E402
from sinks.redis_live import LivePolicy  # noqa: E402

if TYPE_CHECKING:
    import redis

NS = "hecate:test-live"
STREAM, STATE, PRODUCER, CONSUMERS = (
    f"{NS}:{part}" for part in ("stream", "state", "producer", "consumers")
)

QUICK = LivePolicy(
    lease_seconds=0.6,
    heartbeat_seconds=0.05,
    trim_interval_seconds=0.1,
    backpressure_check_seconds=0.02,
)

#: Nothing listens here.
DEAD_ENDPOINT = "ws://127.0.0.1:1"

MAIN = grow(ANCHOR, 12)


def run(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(scenario())


@asynccontextmanager
async def following(
    node: FakeOgmios, policy: LivePolicy = QUICK, **kwargs: Any
) -> AsyncIterator[asyncio.Task[None]]:
    """A follower running beside the test, stopped cleanly at the end."""
    stop = asyncio.Event()
    task = asyncio.create_task(
        live.follow(
            namespace=NS,
            endpoints=[node.url],
            policy=policy,
            stop=stop,
            retry_delay_seconds=0.05,
            **kwargs,
        )
    )
    try:
        yield task
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)


async def until(
    condition: Callable[[], bool], *followers: asyncio.Task[None], timeout: float = 10
) -> None:
    """Wait for ``condition``, surfacing a follower's failure rather than a timeout."""
    deadline = time.monotonic() + timeout
    while not condition():
        for task in followers:
            if task.done():
                task.result()
                raise AssertionError("the follower stopped before the condition held")
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting for the stream")
        await asyncio.sleep(0.02)


def tail(conn: redis.Redis) -> str | None:
    return cast("str | None", conn.hget(STATE, "last_hash"))


def state(conn: redis.Redis) -> dict[str, str]:
    return cast(dict[str, str], conn.hgetall(STATE))


def stream(conn: redis.Redis) -> list[tuple[str, str]]:
    """What a consumer reads, reduced to (type, hash) per entry."""
    entries = cast(list[tuple[str, dict[str, str]]], conn.xrange(STREAM))
    return [(fields["type"], fields["hash"]) for _, fields in entries]


def blocks(chain: list[FakeBlock]) -> list[tuple[str, str]]:
    return [("block", block.hash) for block in chain]


def rollback(to: FakeBlock) -> list[tuple[str, str]]:
    return [("rollback", to.hash)]


def assert_chained(conn: redis.Redis, start: FakeBlock) -> None:
    """The order invariant: every block extends whatever entry precedes it."""
    previous = start.hash
    for _, fields in cast(list[tuple[str, dict[str, str]]], conn.xrange(STREAM)):
        if fields["type"] == "block":
            assert fields["ancestor"] == previous, fields["hash"]
        previous = fields["hash"]


# -- following ----------------------------------------------------------------


def test_a_fresh_stream_starts_after_the_point_and_holds_back_depth(
    conn: redis.Redis,
) -> None:
    chain = [ANCHOR, *MAIN[:6]]

    async def scenario() -> None:
        async with (
            FakeOgmios(chain) as node,
            following(node, start=ANCHOR.point) as task,
        ):
            await until(lambda: tail(conn) == MAIN[3].hash, task)

            # Waiting at the tip for a block that never comes: the newest two
            # stay held back, and the heartbeat carries on regardless.
            beat = float(state(conn)["heartbeat_ts"])
            await asyncio.sleep(0.3)
            assert tail(conn) == MAIN[3].hash
            assert float(state(conn)["heartbeat_ts"]) > beat

    run(scenario)

    assert stream(conn) == blocks(MAIN[:4])
    assert_chained(conn, ANCHOR)
    fields = state(conn)
    assert (fields["tip_slot"], fields["tip_height"]) == (
        str(MAIN[5].slot),
        str(MAIN[5].height),
    )
    assert (fields["depth"], fields["paused"]) == ("2", "0")
    assert fields["producer_id"] and fields["started_at"]
    assert conn.get(PRODUCER) is None, "the lease outlived a clean stop"


@pytest.mark.parametrize("omit_rollbacks", [False, True], ids=["announced", "implied"])
def test_a_rollback_inside_the_buffer_never_reaches_the_stream(
    conn: redis.Redis, omit_rollbacks: bool
) -> None:
    fork = grow(MAIN[4], 3, fork="fork")

    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN[:6]], omit_rollbacks=omit_rollbacks) as node,
            following(node, start=ANCHOR.point) as task,
        ):
            await until(lambda: tail(conn) == MAIN[3].hash, task)
            await node.set_chain([ANCHOR, *MAIN[:5], *fork])
            await until(lambda: tail(conn) == fork[0].hash, task)

    run(scenario)

    assert stream(conn) == blocks(MAIN[:5] + fork[:1])
    assert_chained(conn, ANCHOR)


@pytest.mark.parametrize("omit_rollbacks", [False, True], ids=["announced", "implied"])
def test_a_rollback_below_the_buffer_is_published(
    conn: redis.Redis, omit_rollbacks: bool
) -> None:
    fork = grow(MAIN[3], 6, fork="fork")

    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN[:8]], omit_rollbacks=omit_rollbacks) as node,
            following(node, start=ANCHOR.point) as task,
        ):
            await until(lambda: tail(conn) == MAIN[5].hash, task)
            await node.set_chain([ANCHOR, *MAIN[:4], *fork])
            await until(lambda: tail(conn) == fork[3].hash, task)

    run(scenario)

    assert stream(conn) == blocks(MAIN[:6]) + rollback(MAIN[3]) + blocks(fork[:4])
    assert_chained(conn, ANCHOR)
    rolled = [
        fields
        for _, fields in cast(list[tuple[str, dict[str, str]]], conn.xrange(STREAM))
        if fields["type"] == "rollback"
    ]
    assert rolled[0]["tip_slot"] == str(fork[-1].slot)


def test_a_restart_rolls_back_a_tail_orphaned_while_it_was_down(
    conn: redis.Redis,
) -> None:
    fork = grow(MAIN[2], 8, fork="fork")

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:8]]) as node:
            async with following(node, start=ANCHOR.point) as task:
                await until(lambda: tail(conn) == MAIN[5].hash, task)

            await node.set_chain([ANCHOR, *MAIN[:3], *fork])

            async with following(node) as task:
                await until(lambda: tail(conn) == fork[5].hash, task)

            assert node.intersection_requests[-1][0] == (MAIN[5].slot, MAIN[5].hash)

    run(scenario)

    assert stream(conn) == blocks(MAIN[:6]) + rollback(MAIN[2]) + blocks(fork[:6])
    assert_chained(conn, ANCHOR)


def test_a_restart_on_an_intact_tail_carries_straight_on(conn: redis.Redis) -> None:
    """Blocks held back when the first run stopped were never published, so
    they arrive once, from the second run, with no rollback in between."""

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:6]]) as node:
            async with following(node, start=ANCHOR.point) as task:
                await until(lambda: tail(conn) == MAIN[3].hash, task)

            await node.set_chain([ANCHOR, *MAIN[:10]])

            async with following(node) as task:
                await until(lambda: tail(conn) == MAIN[7].hash, task)

    run(scenario)

    assert stream(conn) == blocks(MAIN[:8])
    assert_chained(conn, ANCHOR)


@pytest.mark.parametrize("start", [ANCHOR, MAIN[5]], ids=["catching-up", "at-the-tip"])
def test_the_tip_block_sent_twice_is_relayed_once(
    conn: redis.Redis, start: FakeBlock
) -> None:
    """dolos re-sends its tip block when a session first reaches it."""

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:6]], repeat_tip=True) as node:
            async with following(node, start=start.point) as task:
                # Sent ahead of anything the extension brings.
                await until(lambda: node.tip_repeats == 1, task)
                await node.set_chain([ANCHOR, *MAIN])
                await until(lambda: tail(conn) == MAIN[9].hash, task)

    run(scenario)

    first = 0 if start is ANCHOR else MAIN.index(start) + 1
    assert stream(conn) == blocks(MAIN[first:10])
    assert_chained(conn, start)


def test_a_dropped_connection_resumes_from_the_tail(conn: redis.Redis) -> None:
    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN[:6]]) as node,
            following(node, start=ANCHOR.point) as task,
        ):
            await until(lambda: tail(conn) == MAIN[3].hash, task)
            await node.drop_connections()
            await node.set_chain([ANCHOR, *MAIN[:10]])
            await until(lambda: tail(conn) == MAIN[7].hash, task)

            assert node.connections_served >= 2
            assert node.intersection_requests[-1][0] == (MAIN[3].slot, MAIN[3].hash)

    run(scenario)

    assert stream(conn) == blocks(MAIN[:8])
    assert_chained(conn, ANCHOR)


def test_a_waiting_standby_takes_over_once_the_lease_is_released(
    conn: redis.Redis,
) -> None:
    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:6]]) as node:
            first_stop = asyncio.Event()
            first = asyncio.create_task(
                live.follow(
                    namespace=NS,
                    start=ANCHOR.point,
                    endpoints=[node.url],
                    policy=QUICK,
                    stop=first_stop,
                )
            )
            await until(lambda: tail(conn) == MAIN[3].hash, first)
            writer = state(conn)["producer_id"]

            async with following(node) as standby:
                await asyncio.sleep(0.3)
                assert state(conn)["producer_id"] == writer

                first_stop.set()
                await asyncio.wait_for(first, timeout=10)
                await node.set_chain([ANCHOR, *MAIN[:10]])
                await until(lambda: tail(conn) == MAIN[7].hash, standby)
                assert state(conn)["producer_id"] != writer

    run(scenario)

    assert stream(conn) == blocks(MAIN[:8])
    assert_chained(conn, ANCHOR)


def test_a_follower_whose_lease_is_taken_stops_fenced_out(conn: redis.Redis) -> None:
    """Even blocked at the tip with nothing to write, it finds out on its next
    heartbeat — and leaves the lease with whoever took it."""

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:6]]) as node:
            task = asyncio.create_task(
                live.follow(
                    namespace=NS,
                    start=ANCHOR.point,
                    endpoints=[node.url],
                    policy=QUICK,
                )
            )
            await until(lambda: tail(conn) == MAIN[3].hash, task)
            conn.set(PRODUCER, "another-follower")

            with pytest.raises(LeaseLostError) as raised:
                await asyncio.wait_for(task, timeout=5)
            assert raised.value.exit_code == 16

    run(scenario)

    assert conn.get(PRODUCER) == "another-follower"


def test_a_lagging_consumer_pauses_the_follower_until_it_goes_stale(
    conn: redis.Redis,
) -> None:
    policy = LivePolicy(
        lease_seconds=0.6,
        heartbeat_seconds=0.05,
        max_unconsumed_blocks=2,
        backpressure_check_seconds=0,
    )
    register(conn, ANCHOR)

    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN]) as node,
            following(node, policy=policy, start=ANCHOR.point) as task,
        ):
            await until(lambda: state(conn).get("paused") == "1", task)
            paused_at = tail(conn)
            beat = float(state(conn)["heartbeat_ts"])
            await asyncio.sleep(0.3)
            assert tail(conn) == paused_at == MAIN[2].hash
            assert float(state(conn)["heartbeat_ts"]) > beat

            register(conn, MAIN[2], age_seconds=3600)
            await until(lambda: tail(conn) == MAIN[9].hash, task)
            assert state(conn)["paused"] == "0"

    run(scenario)

    assert stream(conn) == blocks(MAIN[:10])


def register(
    conn: redis.Redis,
    block: FakeBlock,
    *,
    group: str = "reader",
    age_seconds: float = 0,
) -> None:
    """Stand in for a consumer that anchored at ``block`` and read nothing here."""
    conn.hset(
        CONSUMERS,
        group,
        orjson.dumps(
            {
                "slot": block.slot,
                "hash": block.hash,
                "entry_id": None,
                "updated_at": time.time() - age_seconds,
            }
        ),
    )


def test_an_empty_stream_starts_from_the_slowest_consumer_anchor(
    conn: redis.Redis,
) -> None:
    register(conn, MAIN[1], group="slow")
    register(conn, MAIN[3], group="fast")

    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN[:6]]) as node,
            following(node) as task,
        ):
            await until(lambda: tail(conn) == MAIN[3].hash, task)

    run(scenario)

    assert stream(conn) == blocks(MAIN[2:4])
    assert_chained(conn, MAIN[1])


def test_a_start_point_the_stream_has_moved_past_is_refused(
    conn: redis.Redis,
) -> None:
    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:6]]) as node:
            async with following(node, start=ANCHOR.point) as task:
                await until(lambda: tail(conn) == MAIN[3].hash, task)

            with pytest.raises(StartPointRefusedError, match="already ends at"):
                await live.follow(
                    namespace=NS,
                    start=MAIN[0].point,
                    endpoints=[node.url],
                    policy=QUICK,
                )

    run(scenario)

    assert stream(conn) == blocks(MAIN[:4])


def test_a_node_still_behind_the_start_point_is_waited_for(
    conn: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not finding a point the node has not reached yet is not a missing
    intersection: nothing is written until the node catches up."""
    monkeypatch.setattr(live, "_NODE_BEHIND_POLL_SECONDS", 0.05)

    async def scenario() -> None:
        async with (
            FakeOgmios([ANCHOR, *MAIN[:2]]) as node,
            following(node, start=MAIN[4].point) as task,
        ):
            await asyncio.sleep(0.3)
            assert not task.done()
            assert stream(conn) == []

            await node.set_chain([ANCHOR, *MAIN[:8]])
            await until(lambda: tail(conn) == MAIN[5].hash, task)

    run(scenario)

    assert stream(conn) == blocks(MAIN[5:6])
    assert_chained(conn, MAIN[4])


# -- resolving a start point --------------------------------------------------


@contextmanager
def fake_kupo(answer: dict[str, Any] | None) -> Iterator[tuple[str, list[str]]]:
    """A kupo whose /checkpoints answers ``answer`` for any slot."""
    requested: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requested.append(self.path)
            body = orjson.dumps(answer)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requested
    finally:
        server.shutdown()
        server.server_close()


def test_an_epoch_start_is_the_last_block_before_the_epoch() -> None:
    answer = {"slot_no": ANCHOR.slot, "header_hash": ANCHOR.hash}
    with fake_kupo(answer) as (url, requested):
        point = live.resolve_start_point(epoch=600, kupo_url=url)

    assert point == ANCHOR.point
    assert requested == [f"/checkpoints/{epoch_start_slot(EpochNumber(600)) - 1}"]


def test_a_slot_kupo_cannot_place_is_refused() -> None:
    with fake_kupo(None) as (url, _):
        with pytest.raises(StartPointRefusedError) as raised:
            live.resolve_start_point(slot=1_000, kupo_url=url)

    assert raised.value.exit_code == 17


@pytest.mark.parametrize(
    "request_",
    [
        {"point": ANCHOR.point, "slot": 1},
        {"slot": 1, "epoch": 600},
        {"slot": 1},
    ],
    ids=["point-and-slot", "slot-and-epoch", "slot-without-kupo"],
)
def test_ambiguous_or_unresolvable_start_requests_are_usage_errors(
    request_: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        live.resolve_start_point(**request_)


# -- the CLI, as a supervisor sees it ------------------------------------------


async def follow_cli(*args: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "cli",
        "follow",
        "--namespace",
        NS,
        "--heartbeat-seconds",
        "0.05",
        "--lease-seconds",
        "0.6",
        *args,
        cwd=REPO_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )


async def exit_of(process: asyncio.subprocess.Process) -> tuple[int, str]:
    output, _ = await asyncio.wait_for(process.communicate(), timeout=60)
    assert process.returncode is not None
    return process.returncode, output.decode()


def point_arg(point: Point) -> str:
    return f"{point.slot}.{point.id}"


def test_no_intersection_exits_15(conn: redis.Redis) -> None:
    elsewhere = Point(slot=ANCHOR.slot + 10, id=block_hash("another chain"))

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:3]]) as node:
            code, output = await exit_of(
                await follow_cli(
                    "--endpoint", node.url, "--from-point", point_arg(elsewhere)
                )
            )
        assert code == 15, output
        assert "holds none of the 1 point(s)" in output

    run(scenario)

    assert stream(conn) == []
    assert conn.get(PRODUCER) is None


def test_a_start_too_far_behind_the_tip_exits_17(conn: redis.Redis) -> None:
    far_ahead = grow(ANCHOR, 2, fork="later", slot_step=3 * 432_000)

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *far_ahead]) as node:
            code, output = await exit_of(
                await follow_cli(
                    "--endpoint", node.url, "--from-point", point_arg(ANCHOR.point)
                )
            )
        assert code == 17, output
        assert "--max-catchup-epochs" in output

    run(scenario)

    assert stream(conn) == []
    assert "last_hash" not in state(conn), "a refused start anchored the stream"


def test_nothing_to_start_from_exits_17_without_touching_ogmios(
    conn: redis.Redis,
) -> None:
    async def scenario() -> None:
        code, output = await exit_of(await follow_cli("--endpoint", DEAD_ENDPOINT))
        assert code == 17, output
        assert "no consumer has registered an anchor" in output

    run(scenario)


def test_a_block_that_breaks_the_chain_exits_18(conn: redis.Redis) -> None:
    impostor = FakeBlock(
        slot=MAIN[2].slot,
        hash=block_hash("impostor"),
        height=MAIN[2].height,
        ancestor=block_hash("nobody"),
    )

    async def scenario() -> None:
        async with FakeOgmios([ANCHOR, *MAIN[:2], impostor]) as node:
            code, output = await exit_of(
                await follow_cli(
                    "--endpoint", node.url, "--from-point", point_arg(ANCHOR.point)
                )
            )
        assert code == 18, output

    run(scenario)

    assert stream(conn) == [], "blocks held back past a break were published"


@pytest.mark.parametrize(
    "args",
    [
        ["--from-point", "not-a-point"],
        ["--from-slot", "5", "--from-epoch", "600", "--kupo-url", "http://127.0.0.1:1"],
        ["--from-slot", "5"],
        ["--lease-seconds", "0.1"],
    ],
    ids=["bad-point", "two-starts", "slot-without-kupo", "lease-too-short"],
)
def test_bad_options_are_usage_errors(args: list[str]) -> None:
    async def scenario() -> None:
        code, output = await exit_of(
            await follow_cli("--endpoint", DEAD_ENDPOINT, *args)
        )
        assert code == 2, output

    run(scenario)


def test_the_cli_follows_from_an_epoch_and_stops_cleanly_on_sigterm(
    conn: redis.Redis,
) -> None:
    answer = {"slot_no": ANCHOR.slot, "header_hash": ANCHOR.hash}

    async def scenario() -> None:
        with fake_kupo(answer) as (kupo_url, requested):
            async with FakeOgmios([ANCHOR, *MAIN[:4]]) as node:
                process = await follow_cli(
                    "--endpoint",
                    node.url,
                    "--from-epoch",
                    "600",
                    "--kupo-url",
                    kupo_url,
                )
                deadline = time.monotonic() + 30
                while tail(conn) != MAIN[1].hash:
                    assert process.returncode is None, await exit_of(process)
                    assert time.monotonic() < deadline, "the CLI relayed nothing"
                    await asyncio.sleep(0.05)

                process.send_signal(signal.SIGTERM)
                code, output = await exit_of(process)

        assert code == 0, output
        assert requested == [f"/checkpoints/{epoch_start_slot(EpochNumber(600)) - 1}"]

    run(scenario)

    assert stream(conn) == blocks(MAIN[:2])
    assert conn.get(PRODUCER) is None, "SIGTERM left the lease to lapse"
