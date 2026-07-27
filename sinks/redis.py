import asyncio
import importlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from enum import Enum, auto
from typing import Any

import orjson as json
from ogmios import Block

from config import settings
from errors import ConsumerNotFinishedError, NoRegisteredConsumerError
from models import BlockHeight, EpochNumber
from sinks.base import prepare_block
from sinks.metrics import MetricsClient, epoch_meta_key, heartbeat
from sinks.stream_cleanup import cleanup_streams_loop, is_stream_fully_consumed

_redis_module = importlib.import_module("redis.asyncio")
aioredis = _redis_module


class _StreamState(Enum):
    """What the startup purge found for one epoch."""

    ABSENT = auto()  # nothing there; the usual case below a window
    CONSUMED = auto()  # every registered group acknowledged everything
    ORPHANED = auto()  # holds data, but no group registered — see purge_orphans


def _summarize_epochs(epochs: list[int]) -> str:
    """Render epoch numbers compactly, collapsing runs into ranges."""
    if not epochs:
        return "none"
    spans: list[tuple[int, int]] = []
    for epoch in epochs:
        if spans and epoch == spans[-1][1] + 1:
            spans[-1] = (spans[-1][0], epoch)
        else:
            spans.append((epoch, epoch))
    return ", ".join(str(lo) if lo == hi else f"{lo}–{hi}" for lo, hi in spans)


# Ping a pooled connection that has been idle this long before reusing it.
# A backfill batch can keep the parent's connection idle for minutes while
# worker processes stream blocks, and a silently dropped TCP connection would
# otherwise surface as a ConnectionError on the next phase-2 command.
_HEALTH_CHECK_INTERVAL_SECONDS = 30


class RedisSink:
    """A plain Redis sink: blocks onto one list, plus a status hash.

    The simple option, and deliberately not the epoch-stream protocol
    ``HistoricalRedisSink`` implements — that one encodes an ordering and
    completion contract a consumer has to opt into. This one just pushes
    ``prepare_block`` payloads onto ``<prefix>blocks`` in order, which any
    ``LPOP``/``BRPOP`` consumer can read with no further ceremony.

    Structurally implements ``sinks.base.DataSink``, so it can be handed to
    ``backfill()`` as a sink factory. It carries no ``EpochCoordinator``
    surface, so such a backfill has no resumability and no backpressure.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        prefix: str = "hecate:",
        **redis_kwargs: Any,
    ):
        """Connect to ``url``, defaulting to ``REDIS_URL`` from the environment."""
        self.redis = aioredis.from_url(url or settings.REDIS_URL, **redis_kwargs)
        self.prefix = prefix
        self.block_queue = f"{self.prefix}blocks"
        self.status_key = f"{self.prefix}status"

    async def __aenter__(self) -> "RedisSink":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def send_block(self, block: Block) -> None:
        """Send a block to Redis."""
        block_data = prepare_block(block)
        await self.redis.rpush(self.block_queue, json.dumps(block_data))
        await self.redis.hset(self.status_key, "last_block_hash", block_data["hash"])
        await self.redis.hset(
            self.status_key, "last_block_slot", str(block_data["slot"])
        )

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Send a batch of blocks to Redis."""
        if not blocks:
            return

        pipeline = self.redis.pipeline()
        for block in blocks:
            pipeline.rpush(self.block_queue, json.dumps(prepare_block(block)))

        # Update status with last block info
        last_block = prepare_block(blocks[-1])
        pipeline.hset(self.status_key, "last_block_hash", last_block["hash"])
        pipeline.hset(self.status_key, "last_block_slot", str(last_block["slot"]))

        await pipeline.execute()

    async def get_status(self) -> dict[str, Any]:
        status = await self.redis.hgetall(self.status_key)
        queue_length = await self.redis.llen(self.block_queue)

        return {
            "queue_length": queue_length,
            "last_block_hash": status.get(b"last_block_hash", b"").decode(),
            "last_block_slot": status.get(b"last_block_slot", b"0").decode(),
        }

    async def close(self) -> None:
        """Close the Redis connection."""
        await self.redis.close()


# Lua script to atomically advance last_synced_epoch
_ADVANCE_EPOCH_LUA = r"""
local last_synced_epoch, ready_set, resume_map = KEYS[1], KEYS[2], KEYS[3]
local cur = tonumber(redis.call("GET", last_synced_epoch))
if cur == nil then
  return redis.error_reply(
    "last_synced_epoch is unset: call ensure_ordering_base() before committing epochs"
  )
end
local next = cur + 1
while redis.call("SISMEMBER", ready_set, tostring(next)) == 1 do
  redis.call("SREM", ready_set, tostring(next))
  redis.call("SET", last_synced_epoch, tostring(next))
  redis.call("HDEL", resume_map, tostring(next))
  next = next + 1
end
return redis.call("GET", last_synced_epoch)
"""


class HistoricalRedisSink:
    """
    A Redis‐backed sink for reliably streaming historical epoch data.

    Each epoch's block batches are written directly to a dedicated per‐epoch
    Redis stream (``<prefix>epoch:{N}``). Control events are logged into a
    separate event stream (``<prefix>event_stream``). Resume positions are
    tracked in a Redis hash (``<prefix>resume_map``) and completed epochs
    awaiting sequential commit live in a Redis set (``<prefix>ready_set``).
    A Lua script atomically advances ``last_synced_epoch`` once all preceding
    epochs are ready, ensuring ordered, at‐least‐once delivery and
    resumability on failure.

    Ordering is guaranteed by construction: each epoch has its own stream,
    and consumers read streams in ascending epoch order.

    Structurally implements both ``BlockRelay`` and ``EpochCoordinator``
    (see ``sinks.base``); the extra surface is what gives a backfill
    resumability, ordering and backpressure.
    """

    def __init__(
        self,
        *,
        prefix: str = "hecate:history:",
        logger: logging.Logger | None = None,
    ):
        self.prefix = prefix

        # Per-epoch stream prefix — each epoch gets its own stream:
        #   hecate:history:epoch:208, hecate:history:epoch:209, …
        self.epoch_stream_prefix = f"{prefix}epoch:"

        # Stream of audit/control events:
        #   • Entries: {"type":"epoch_complete", "epoch":<int>, …}
        self.event_stream = f"{prefix}event_stream"

        # Hash of in‐progress epochs' resume positions:
        #   • Field = epoch_number (as string)
        #   • Value = last_processed_block_height (int)
        self.resume_map = f"{prefix}resume_map"

        # Set of epochs that have been fully processed by a worker,
        # but are waiting for all earlier epochs to complete before
        # advancing `last_synced_epoch`.
        self.ready_set = f"{prefix}ready_set"

        # Highest epoch N such that *all* epochs from start through N
        # have been successfully marked complete and synchronized.
        self.last_synced_epoch = f"{prefix}last_synced_epoch"

        # Lowest epoch whose stream still exists in Redis.
        # Epochs below this have been cleaned up.
        self.low_watermark = f"{prefix}low_watermark"

        self.redis: aioredis.Redis | None = None  # type: ignore
        self.metrics: MetricsClient | None = None
        self._advance_sha: str | None = None
        self.logger = logger or logging.getLogger(__name__)

    async def __aenter__(self) -> "HistoricalRedisSink":
        url = settings.REDIS_URL
        self.redis = aioredis.from_url(
            url,
            decode_responses=False,
            health_check_interval=_HEALTH_CHECK_INTERVAL_SECONDS,
        )
        self.metrics = MetricsClient(self.redis, self.prefix, self.logger)
        self.logger.debug("🔗 Connecting to Redis at %s", url)

        # load our Lua once
        self._advance_sha = await self.redis.script_load(_ADVANCE_EPOCH_LUA)
        self.logger.debug("✅ loaded Lua advance script")
        # Deliberately no writes here: opening this sink to read status must
        # not seed an ordering base, because a base that does not match the
        # window a later run asks for silently strands that run's output.
        # Seeding is `ensure_ordering_base`, called by whoever is relaying.
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.redis:
            await self.redis.close()
            self.logger.debug("🛑 Redis connection closed")

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """XADD a prepared batch payload directly to the per-epoch Redis stream."""
        assert self.redis, "Not initialized"

        epoch = kwargs.pop("epoch")
        last_height = blocks[-1].height

        batch_list = [prepare_block(b) for b in blocks]
        payload = json.dumps(batch_list)

        await self.redis.xadd(
            f"{self.epoch_stream_prefix}{epoch}",
            {"type": "batch", "epoch": str(epoch), "data": payload},
        )

        # Only advance the resume cursor after the XADD is confirmed.
        await self.redis.hset(self.resume_map, epoch, last_height)

        assert self.metrics
        await self.metrics.record_batch_written(
            epoch=epoch,
            block_count=len(blocks),
            payload_bytes=len(payload),
        )

        self.logger.debug(
            "Wrote batch for epoch %s, up to height %s", epoch, last_height
        )

    async def reset_epoch_state(self, epoch: EpochNumber) -> None:
        """Clear epoch stream and resume state for an epoch (used before retries)."""
        assert self.redis, "Not initialized"
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(f"{self.epoch_stream_prefix}{epoch}")
        pipe.hdel(self.resume_map, epoch)
        await pipe.execute()

    async def mark_epoch_complete(
        self,
        epoch: EpochNumber,
        last_height: BlockHeight,
    ) -> EpochNumber:
        """
        Mark an epoch as complete by updating Redis with the relevant information
        and logging the event. This function performs two main tasks:

        1. Adds the epoch to a set of completed epochs and logs the completion event,
            including the last processed block height consumers should expect for this epoch.
        2. Executes the Lua script stored in Redis to update the last synced epoch and
            clean up associated resume data.

        This function ensures synchronization of epoch information and related
        cleanup tasks.

        :param epoch: The epoch number that has been completed.
        :type epoch: EpochNumber
        :param last_height: The last block height processed in the associated epoch.
        :type last_height: BlockHeight
        :return: The updated last synced epoch number after Redis executes the Lua script.
        :rtype: EpochNumber
        """
        assert self.redis and self._advance_sha, "Redis not initialized"
        # 1) enqueue into ready_set & log event
        pipe = self.redis.pipeline(transaction=True)
        pipe.sadd(self.ready_set, epoch)
        pipe.xadd(
            self.event_stream,
            {"type": "epoch_complete", "epoch": epoch, "last_height": last_height},
        )
        await pipe.execute()

        # 2) run Lua to bump last_synced_epoch and cleanup resume_map
        latest_epoch = await self.redis.evalsha(
            self._advance_sha,  # Computed SHA1 of the Lua script
            3,  # Number of keys passed to the script
            self.last_synced_epoch,
            self.ready_set,
            self.resume_map,
        )
        last_synced = EpochNumber(int(latest_epoch))

        assert self.metrics
        await self.metrics.record_epoch_published(
            epoch=epoch,
            stream_key=f"{self.epoch_stream_prefix}{epoch}",
        )

        self.logger.info(
            "Epoch %s marked complete; last_synced_epoch → %s",
            epoch,
            last_synced,
        )
        return last_synced

    async def ensure_ordering_base(self, base: EpochNumber) -> EpochNumber:
        """Seed the ordering state if untouched, and report the base in force.

        ``last_synced_epoch`` is both what consumers read up to and where
        ``_ADVANCE_EPOCH_LUA`` starts walking, so it has to exist before any
        epoch can be committed. Seeding is SETNX: an existing base belongs to
        whoever wrote it, and is returned unchanged for the caller to judge
        against the window it is about to relay — a base below
        ``first_epoch - 1`` can never be walked up to, and every epoch such a
        run publishes would sit unreachable in ``ready_set``.
        """
        assert self.redis, "Not initialized"
        pipe = self.redis.pipeline(transaction=True)
        pipe.setnx(self.last_synced_epoch, base)
        pipe.setnx(self.low_watermark, base + 1)
        await pipe.execute()

        in_force = await self.get_last_synced_epoch()
        assert in_force is not None, "ordering base vanished after seeding"
        return in_force

    async def reset_ordering_base(self, base: EpochNumber) -> None:
        """Move the ordering base, dropping any claim on earlier epochs.

        Only safe once ``purge_stale_streams`` has confirmed nothing below the
        window is still owed to a consumer: this asserts that epochs at or
        below ``base`` are out of scope, which is a lie unless the operator
        means it. Consumers read ``last_synced_epoch`` as "everything through
        here has been delivered".
        """
        assert self.redis, "Not initialized"
        await self.redis.set(self.last_synced_epoch, base)

    async def get_last_synced_epoch(self) -> EpochNumber | None:
        """Highest epoch delivered in order, or None if nothing has been.

        This is the field consumers bound their reads by.
        """
        assert self.redis, "Not initialized"
        val = await self.redis.get(self.last_synced_epoch)
        return EpochNumber(int(val)) if val is not None else None

    async def get_epoch_resume_height(self, epoch: EpochNumber) -> BlockHeight | None:
        assert self.redis, "Not initialized"
        val = await self.redis.hget(self.resume_map, epoch)
        return BlockHeight(int(val)) if val else None

    async def get_low_watermark(self) -> EpochNumber | None:
        """Lowest epoch whose stream still exists, or None if untouched."""
        assert self.redis, "Not initialized"
        val = await self.redis.get(self.low_watermark)
        return EpochNumber(int(val)) if val is not None else None

    async def purge_stale_streams(
        self, up_to_epoch: EpochNumber, *, purge_orphans: bool = False
    ) -> int:
        """Delete epoch streams below ``up_to_epoch`` and advance ``low_watermark``.

        Called at backfill startup to reclaim streams left below the window by
        a prior run. Deletes only what is provably finished with: streams that
        are missing, or that every registered consumer group has fully
        acknowledged.

        A stream with data but **no** registered consumer group is refused by
        default. "No group yet" and "no group ever" are the same state on the
        wire, and a consumer fleet whose workers register independently may
        simply not have started: dropping those epochs loses data with no
        error on either side. ``purge_orphans`` opts into dropping them, for
        recovering the namespace of a run whose consumers never existed.

        Raises ``UnsafePurgeError`` rather than deleting anything a consumer is
        still owed.

        Returns the number of streams actually deleted, which is usually
        fewer than the epochs swept: the range below a window is mostly
        epochs that were never published here.
        """
        assert self.redis, "Not initialized"
        low_wm = await self.get_low_watermark()

        if low_wm is None or low_wm >= up_to_epoch:
            return 0

        swept = range(low_wm, up_to_epoch)
        present: list[int] = []
        orphaned: list[int] = []
        for epoch in swept:
            state = await self._stream_purge_state(epoch, purge_orphans=purge_orphans)
            if state is _StreamState.ORPHANED:
                orphaned.append(epoch)
            if state is not _StreamState.ABSENT:
                present.append(epoch)

        if orphaned:
            self.logger.warning(
                "⚠️  Dropping %d epoch stream(s) with no registered consumer "
                "group (%s): if a consumer meant to read them, that data is "
                "gone",
                len(orphaned),
                _summarize_epochs(orphaned),
            )

        pipe = self.redis.pipeline(transaction=True)
        for epoch in swept:
            pipe.delete(f"{self.epoch_stream_prefix}{epoch}")
            pipe.delete(epoch_meta_key(self.prefix, epoch))
            pipe.hdel(self.resume_map, epoch)
            pipe.srem(self.ready_set, epoch)
        pipe.set(self.low_watermark, up_to_epoch)
        await pipe.execute()

        if present:
            self.logger.info(
                "Purged %d stale epoch stream(s) of %d–%d; low_watermark → %d",
                len(present),
                low_wm,
                up_to_epoch - 1,
                up_to_epoch,
            )
        else:
            self.logger.info(
                "No epoch streams present below %d; low_watermark → %d",
                up_to_epoch,
                up_to_epoch,
            )
        return len(present)

    async def _stream_purge_state(
        self, epoch: int, *, purge_orphans: bool
    ) -> "_StreamState":
        """Classify an epoch stream for purging, raising if it is still owed."""
        assert self.redis, "Not initialized"
        stream_key = f"{self.epoch_stream_prefix}{epoch}"

        if not await self.redis.exists(stream_key):
            return _StreamState.ABSENT

        groups: list[dict[str, Any]] = await self.redis.xinfo_groups(stream_key)
        if not groups:
            if purge_orphans:
                return _StreamState.ORPHANED
            raise NoRegisteredConsumerError(epoch, stream_key)

        if await is_stream_fully_consumed(self.redis, stream_key):
            return _StreamState.CONSUMED

        raise ConsumerNotFinishedError(epoch, stream_key, len(groups))

    async def _consumer_lag(self) -> int:
        """Epochs published but not yet consumed (``last_synced - low_watermark``)."""
        last_synced = await self.get_last_synced_epoch()
        low_wm = await self.get_low_watermark()
        if last_synced is None or low_wm is None:
            return 0
        return last_synced - low_wm

    async def wait_for_backpressure(self) -> None:
        """Block until consumers have caught up enough to accept more epoch data.

        Pauses when consumer lag reaches ``max_unconsumed_epochs``. Logs +
        publishes the pause edge once; subsequent polls just sleep. The
        resume edge is published in ``finally`` so it fires on any exit.
        """
        assert self.metrics, "Not initialized"
        paused = False
        try:
            while (
                lag := await self._consumer_lag()
            ) >= settings.REDIS_MAX_UNCONSUMED_EPOCHS:
                if paused:
                    await asyncio.sleep(10)
                    continue
                paused = True
                await self.metrics.note_backpressure_pause()
                self.logger.warning(
                    "Backpressure: %d unconsumed epochs (limit %d). Waiting…",
                    lag,
                    settings.REDIS_MAX_UNCONSUMED_EPOCHS,
                )
                await asyncio.sleep(10)
        finally:
            if paused:
                await self.metrics.note_backpressure_resume()

    async def note_batch_started(self, *, active: int, maximum: int) -> None:
        """Record that a concurrent batch of epoch workers is in flight."""
        assert self.metrics, "Not initialized"
        await self.metrics.note_workers_busy(active=active, maximum=maximum)

    async def note_batch_finished(self) -> None:
        """Record that no epoch workers are running."""
        assert self.metrics, "Not initialized"
        await self.metrics.note_workers_idle()

    @asynccontextmanager
    async def run_bookkeeping(
        self, *, target_epoch: EpochNumber
    ) -> AsyncIterator[None]:
        """Run this sink's background upkeep for the duration of the body.

        Two loops the dashboard and Redis depend on, owned here rather than
        by the backfill because both are Redis-shaped concerns:

        * a 1Hz liveness heartbeat, so a dead producer is detectable;
        * stream cleanup, deleting per-epoch streams every consumer group
          has fully acknowledged and advancing ``low_watermark``.

        Both hold their own Redis connections so they outlive any
        open/close cycle on this sink's own connection.
        """
        cleanup = asyncio.create_task(
            cleanup_streams_loop(
                prefix=self.prefix, target_epoch=target_epoch, logger=self.logger
            )
        )
        try:
            async with heartbeat(settings.REDIS_URL, self.prefix):
                yield
        finally:
            cleanup.cancel()
            # Awaiting lets the loop close its own Redis connection.
            with suppress(asyncio.CancelledError):
                await cleanup

    async def get_status(self) -> dict[str, Any]:
        """Report progress without touching it — this is a read-only call.

        ``ordering_consistent`` checks the one invariant that cannot hold if
        an ordering base was seeded below a window that was later relayed:
        ``low_watermark`` may sit at most one epoch above
        ``last_synced_epoch``, since nothing can be reclaimed before it has
        been delivered. False means epochs are stranded in ``ready_set``
        where no consumer bounded by ``last_synced_epoch`` can reach them.
        """
        assert self.redis, "Not initialized"
        last_synced = await self.get_last_synced_epoch()
        low_wm = await self.get_low_watermark()
        return {
            "last_synced_epoch": last_synced,
            "low_watermark": low_wm,
            "epochs_individually_completed_pending_sync": await self.redis.scard(
                self.ready_set
            ),
            "epochs_with_active_resume_points": await self.redis.hlen(self.resume_map),
            "ordering_consistent": (
                last_synced is None or low_wm is None or low_wm <= last_synced + 1
            ),
            "redis_connection": "ok",
        }

    async def close(self) -> None:
        await self.__aexit__(None, None, None)

    async def send_block(self, block: Block) -> None:
        """This method is not used in this class, as we only send batches of blocks instead."""
        raise NotImplementedError
