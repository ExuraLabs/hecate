"""Live follow: relay the chain block by block, from a start point to the tip and on.

The counterpart of ``backfill``: where that relays finished epochs in bulk,
this stays at the tip and relays each block as the node adopts it, rollbacks
included, into one Redis stream any number of consumers read::

    from follow import follow

    await follow(namespace="hecate:live", start=Point(slot=..., id=...))

It runs until ``stop`` is set, or until something happens that a restart
would not fix — each of those is a ``errors.FollowError`` with its own exit
code. The stream it writes is the contract in ``docs/live-stream.md``.

One writer per namespace: a follower first takes the producer lease, and
waits as standby while another holds it. Ogmios dropping the connection is
routine — the follower reconnects, rotating through the configured
endpoints, and re-intersects at the stream's own tail.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Sequence
from typing import Any

import requests
from ogmios import Block, Point, Tip
from redis.exceptions import RedisError
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from backfill import fast_block_init
from client import HecateClient
from client.chainsync import RollForward
from constants import DEFAULT_LIVE_NAMESPACE
from epoch_derivation import SHELLEY_EPOCH_LENGTH, epoch_start_slot, kupo_point_at
from errors import IntersectionNotFoundError, StartPointRefusedError, TailMovedError
from models import BlockHash, EpochNumber, Slot
from network import NetworkManager
from sinks.base import BufferedSink
from sinks.redis_live import LivePolicy, RedisLiveSink

logger = logging.getLogger(__name__)

# Blocks off the tip are relayed in the backfill's wire format, so they are
# built the way the backfill builds them.
Block.__init__ = fast_block_init

__all__ = [
    "DEFAULT_DEPTH",
    "DEFAULT_IN_FLIGHT",
    "DEFAULT_MAX_CATCHUP_EPOCHS",
    "follow",
    "resolve_start_point",
]

#: Blocks held back from the stream until that many have been built on them.
DEFAULT_DEPTH = 2

#: nextBlock requests kept outstanding against Ogmios.
DEFAULT_IN_FLIGHT = 50

#: How far behind the tip a start point may be. The live path relays one
#: stream entry per block for every consumer to replay; catching up across
#: epochs is what ``backfill``'s per-epoch streams are for.
DEFAULT_MAX_CATCHUP_EPOCHS = 2

#: Points offered one after another from the tail when resuming; beyond these
#: they thin out exponentially, down to the security parameter.
_DENSE_RESUME_POINTS = 16

#: How often to re-ask a node that is behind the stream whether it caught up.
_NODE_BEHIND_POLL_SECONDS = 10.0

#: Transport failures: reconnect and re-intersect, never exit.
_TRANSIENT = (ConnectionClosed, InvalidHandshake, OSError, TimeoutError)


def resolve_start_point(
    *,
    point: Point | None = None,
    slot: int | None = None,
    epoch: int | None = None,
    kupo_url: str | None = None,
) -> Point | None:
    """Turn an explicit start request into the point to intersect at.

    At most one of the three may be given. A point is taken as is; a slot is
    the last block at or before it, and an epoch the last block before it
    starts — both found through kupo, so the first block relayed is the one
    right after. None when nothing was asked for.

    :raises ValueError: for more than one request, or a slot or epoch without
        ``kupo_url``.
    :raises StartPointRefusedError: if kupo cannot resolve the slot.
    """
    given = [value for value in (point, slot, epoch) if value is not None]
    if len(given) > 1:
        raise ValueError("give at most one of a start point, slot or epoch")
    if point is not None:
        return point
    if slot is not None:
        target = Slot(slot)
    elif epoch is not None:
        target = Slot(epoch_start_slot(EpochNumber(epoch)) - 1)
    else:
        return None
    if kupo_url is None:
        raise ValueError("a start slot or epoch is resolved through kupo")

    try:
        return kupo_point_at(kupo_url, target)
    except (requests.RequestException, LookupError, ValueError) as exc:
        raise StartPointRefusedError(
            f"kupo could not resolve slot {target} to a block: {exc}"
        ) from exc


async def follow(
    *,
    namespace: str = DEFAULT_LIVE_NAMESPACE,
    start: Point | None = None,
    endpoints: Sequence[str] | None = None,
    depth: int = DEFAULT_DEPTH,
    in_flight: int = DEFAULT_IN_FLIGHT,
    max_catchup_epochs: int = DEFAULT_MAX_CATCHUP_EPOCHS,
    policy: LivePolicy | None = None,
    redis_url: str | None = None,
    stop: asyncio.Event | None = None,
    retry_delay_seconds: float = 1.0,
    max_retry_delay_seconds: float = 30.0,
) -> None:
    """Relay the chain into the live stream under ``namespace`` until stopped.

    :param start: Where to start when the stream is empty — see "Start point"
        in docs/live-stream.md. A non-empty stream always resumes from its own
        tail; naming any other point then is refused.
    :param endpoints: Ogmios endpoints, rotated on every reconnect. Defaults
        to ``OGMIOS_ENDPOINTS`` from the environment.
    :param depth: Blocks held back from the stream until that many more have
        been built on them; rollbacks that shallow never reach consumers.
    :param in_flight: nextBlock requests kept outstanding.
    :param max_catchup_epochs: Refuse a start point further than this many
        epochs behind the node's tip.
    :param policy: Lease, heartbeat, backpressure and retention knobs.
    :param redis_url: Defaults to ``REDIS_URL`` from the environment.
    :param stop: Set it to stop gracefully: the lease is released so a
        standby takes over at once, and whatever was held back is simply
        relayed again by whoever resumes.
    :raises errors.FollowError: for anything a restart would not fix — or,
        for ``FencedOutError``, anything only a restart can.
    """
    stop = stop or asyncio.Event()
    async with RedisLiveSink(
        namespace=namespace, url=redis_url, policy=policy, depth=depth
    ) as sink:
        if not await sink.wait_for_lease(stop):
            return
        relay = _Relay(
            sink,
            start=start,
            network=NetworkManager(endpoints),
            depth=depth,
            in_flight=in_flight,
            max_catchup_epochs=max_catchup_epochs,
            retry_delay_seconds=retry_delay_seconds,
            max_retry_delay_seconds=max_retry_delay_seconds,
        )
        try:
            await _run_until_stopped(stop, sink.run_upkeep(), relay.run())
        finally:
            try:
                await sink.release_lease()
            except RedisError as exc:
                # Must not mask whatever ended the run; the lease lapses anyway.
                logger.warning(
                    "Could not release the producer lease (%s); it lapses within %.0fs",
                    exc,
                    sink.policy.lease_seconds,
                )
    logger.info("Stopped following %s", namespace)


async def _run_until_stopped(
    stop: asyncio.Event, *work: Coroutine[Any, Any, None]
) -> None:
    """Run ``work`` side by side until ``stop`` is set or any of it fails."""
    tasks = [asyncio.create_task(coroutine) for coroutine in work]
    stopper = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            [*tasks, stopper], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for task in (*tasks, stopper):
            task.cancel()
        await asyncio.gather(*tasks, stopper, return_exceptions=True)
    for task in tasks:
        if task in done:
            task.result()


def _spread(points: list[Point]) -> list[Point]:
    """Thin a newest-first run of points into a findIntersection request.

    Every point near the tail, then every power of two back, then the oldest:
    the intersection found is at most twice as deep as the real fork, from a
    request of a few dozen points rather than thousands.
    """
    chosen = [
        point
        for index, point in enumerate(points)
        if index < _DENSE_RESUME_POINTS or index & (index - 1) == 0
    ]
    if chosen[-1] is not points[-1]:
        chosen.append(points[-1])
    return chosen


def _describe(point: Point) -> str:
    return f"{point.slot}.{point.id}"


class _Relay:
    """Ogmios to the stream, across reconnects."""

    def __init__(
        self,
        sink: RedisLiveSink,
        *,
        start: Point | None,
        network: NetworkManager,
        depth: int,
        in_flight: int,
        max_catchup_epochs: int,
        retry_delay_seconds: float,
        max_retry_delay_seconds: float,
    ):
        self.sink = sink
        self.start = start
        self.network = network
        self.depth = depth
        self.in_flight = in_flight
        self.max_catchup_epochs = max_catchup_epochs
        self.retry_delay_seconds = retry_delay_seconds
        self.max_retry_delay_seconds = max_retry_delay_seconds
        #: Whether the start point has been settled; after that, every
        #: connection resumes from the stream's tail.
        self.started = False

    async def run(self) -> None:
        delay = self.retry_delay_seconds
        while True:
            # Read from the stream before connecting: a start that has to be
            # refused is refused whether or not Ogmios is reachable.
            if self.started:
                candidates, tail = await self._tail_candidates()
            else:
                candidates, tail = await self._start_candidates()

            endpoint = self.network.get_connection()
            try:
                async with HecateClient(endpoint_url=endpoint) as client:
                    intersection = await self._intersect(client, candidates, tail)
                    delay = self.retry_delay_seconds
                    await self._relay(client, intersection)
            except _TRANSIENT as exc:
                logger.warning(
                    "Lost Ogmios at %s (%s: %s); reconnecting in %.0fs",
                    endpoint,
                    type(exc).__name__,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.max_retry_delay_seconds)

    async def _relay(self, client: HecateClient, intersection: Point) -> None:
        buffer = BufferedSink(self.sink, base=intersection, depth=self.depth)
        async for event in client.next_block.follow(
            intersection, in_flight=self.in_flight
        ):
            await self.sink.note_tip(event.tip)
            if isinstance(event, RollForward):
                await buffer.send_block(event.block, tip=event.tip)
            else:
                if event.point != buffer.head:
                    logger.info(
                        "Node rolled back to %s (tip %s)",
                        _describe(event.point),
                        event.tip.slot,
                    )
                await buffer.rollback_to(event.point, tip=event.tip)
            await self.sink.wait_for_backpressure()

    async def _intersect(
        self, client: HecateClient, candidates: list[Point], tail: Point | None
    ) -> Point:
        """Place the chain-sync cursor, reconciling the stream with the node.

        Offers the node ``candidates``: the stream's canonical points, newest
        first, or on an empty stream (``tail`` None) the start point. An
        intersection below the tail means the tail was orphaned while nobody
        was relaying: a rollback to the intersection is published before
        anything else. Nothing is written at all until the node is known to be
        at least as far along as the stream, so a node still syncing is
        waited for rather than mistaken for a fork.
        """
        while True:
            found, tip = await client.find_intersection.locate(candidates)
            if found is not None and (tail is None or found == tail):
                break
            if tip.slot < candidates[0].slot:
                logger.warning(
                    "Node tip %s is behind %s; waiting for it to catch up",
                    tip.slot,
                    _describe(candidates[0]),
                )
                await asyncio.sleep(_NODE_BEHIND_POLL_SECONDS)
                continue
            if found is None:
                raise IntersectionNotFoundError(points=candidates, tip=tip)
            break

        await self.sink.note_tip(tip)
        if not self.started:
            self._check_catchup(found, tip)
            if tail is None:
                await self.sink.anchor_empty_stream(found)
            self.started = True

        if tail is not None and found != tail:
            await self.sink.send_rollback(found, tip=tip)
        logger.info("Following from %s; node tip %s", _describe(found), tip.slot)
        return found

    async def _start_candidates(self) -> tuple[list[Point], Point | None]:
        """Points to intersect at on first start, and the stream's tail if any.

        In order: a non-empty stream resumes from its tail; otherwise the
        explicit start; otherwise the slowest registered consumer's anchor.
        """
        if await self.sink.stream_length() > 0:
            canonical = [
                point.to_point() for point in await self.sink.canonical_points()
            ]
            tail = canonical[0]
            await self._check_state(tail)
            if self.start is not None and self.start != tail:
                raise StartPointRefusedError(
                    f"asked to start from {_describe(self.start)}, but "
                    f"{self.sink.keys.stream} already ends at {_describe(tail)}. "
                    f"Drop the start option to resume from the stream's tail, "
                    f"or use a fresh namespace"
                )
            self.sink.adopt_tail(tail)
            logger.info(
                "Resuming %s from its tail %s", self.sink.keys.stream, _describe(tail)
            )
            return _spread(canonical), tail

        if self.start is not None:
            logger.info("Starting an empty stream from %s", _describe(self.start))
            return [self.start], None

        anchors = await self.sink.consumer_anchors()
        if anchors:
            slowest = min(anchors, key=lambda anchor: anchor.slot)
            point = Point(slot=slowest.slot, id=slowest.hash)
            logger.info(
                "Starting an empty stream from consumer %s's anchor %s",
                slowest.group,
                _describe(point),
            )
            return [point], None

        raise StartPointRefusedError(
            f"{self.sink.keys.stream} is empty, no start point was given and "
            f"no consumer has registered an anchor in {self.sink.keys.consumers}"
        )

    async def _tail_candidates(self) -> tuple[list[Point], Point]:
        """Points to re-intersect at after a reconnect, and the stream's tail."""
        if await self.sink.stream_length() > 0:
            canonical = [
                point.to_point() for point in await self.sink.canonical_points()
            ]
        else:
            state = await self.sink.read_state()
            canonical = [
                Point(
                    slot=Slot(int(state["last_slot"])), id=BlockHash(state["last_hash"])
                )
            ]
        tail = canonical[0]
        if self.sink.tail != tail:
            raise TailMovedError(
                expected=_describe(self.sink.tail) if self.sink.tail else "",
                found=_describe(tail),
            )
        return _spread(canonical), tail

    async def _check_state(self, tail: Point) -> None:
        """The state hash is written with every entry, so it must agree."""
        state = await self.sink.read_state()
        recorded = (state.get("last_slot"), state.get("last_hash"))
        if recorded != (str(tail.slot), tail.id):
            raise StartPointRefusedError(
                f"{self.sink.keys.state} records the tail as "
                f"{recorded[0]}.{recorded[1]}, but the stream ends at "
                f"{_describe(tail)}: something other than a follower wrote here"
            )

    def _check_catchup(self, point: Point, tip: Tip) -> None:
        behind = tip.slot - point.slot
        allowed = self.max_catchup_epochs * SHELLEY_EPOCH_LENGTH
        if behind > allowed:
            raise StartPointRefusedError(
                f"{_describe(point)} is {behind} slots "
                f"({behind / SHELLEY_EPOCH_LENGTH:.1f} epochs) behind the node's "
                f"tip at {tip.slot}, past the {self.max_catchup_epochs} epoch(s) "
                f"--max-catchup-epochs allows. Relay that stretch with backfill, "
                f"or raise the limit for this start"
            )
