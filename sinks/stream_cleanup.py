"""Reclaim per-epoch Redis streams once consumers are done with them.

Runs as a background ``asyncio.Task`` alongside a backfill. Nothing here
knows about orchestration — it is pure asyncio + Redis.
"""

import asyncio
import logging
from typing import Any

from redis.asyncio import Redis

from config import settings
from sinks.metrics import MetricsClient, epoch_meta_key

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check interval


async def is_stream_fully_consumed(redis: Any, stream_key: str) -> bool:
    """Return True if all consumer groups have fully consumed the stream.

    A stream is considered fully consumed when:
    - At least one consumer group exists
    - Every group has 0 pending entries
    - Every group's ``last-delivered-id`` equals the stream's ``last-generated-id``
    """
    stream_info: dict[str, Any] = await redis.xinfo_stream(stream_key)
    last_generated = stream_info.get("last-generated-id", b"0-0")
    if isinstance(last_generated, bytes):
        last_generated = last_generated.decode()

    groups: list[dict[str, Any]] = await redis.xinfo_groups(stream_key)
    if not groups:
        return False

    for group in groups:
        pending = group.get("pending", 0)
        if pending > 0:
            return False
        last_delivered = group.get("last-delivered-id", b"0-0")
        if isinstance(last_delivered, bytes):
            last_delivered = last_delivered.decode()
        if last_delivered != last_generated:
            return False

    return True


class EpochStreamKeys:
    """Redis key naming for epoch streams under a given sink prefix."""

    def __init__(self, prefix: str):
        self.prefix = prefix

    @property
    def low_watermark(self) -> str:
        return f"{self.prefix}low_watermark"

    @property
    def last_synced_epoch(self) -> str:
        return f"{self.prefix}last_synced_epoch"

    def epoch_stream(self, epoch: int) -> str:
        return f"{self.prefix}epoch:{epoch}"

    def epoch_meta(self, epoch: int) -> str:
        return epoch_meta_key(self.prefix, epoch)


async def _get_boundaries(
    redis: Redis, keys: EpochStreamKeys
) -> tuple[int, int] | None:
    """Read low watermark and last synced epoch from Redis.

    Returns:
        Tuple of (low_watermark, last_synced_epoch) or None if not available.
    """
    raw_low = await redis.get(keys.low_watermark)
    raw_synced = await redis.get(keys.last_synced_epoch)

    if raw_low is None or raw_synced is None:
        return None

    return int(raw_low), int(raw_synced)


async def _cleanup_consumed_epochs(
    redis: Redis,
    keys: EpochStreamKeys,
    metrics: MetricsClient,
    low_wm: int,
    last_synced: int,
    logger: logging.Logger,
) -> None:
    """Iterate through epochs and delete fully consumed streams.

    Stops at the first unconsumed epoch because reads are sequential,
    meaning subsequent epochs are also not consumed yet.  The
    ``last_synced`` epoch stream is always retained so that
    ``low_watermark`` never exceeds ``last_synced_epoch``.

    Streams that no longer exist (e.g. cleaned up by a prior run) are
    skipped and the watermark is advanced past them.
    """
    for epoch in range(low_wm, last_synced):
        stream_key = keys.epoch_stream(epoch)

        if not await redis.exists(stream_key):
            await redis.set(keys.low_watermark, epoch + 1)
            logger.debug(
                "Epoch stream %s already removed; low_watermark -> %d",
                stream_key,
                epoch + 1,
            )
            continue

        if await is_stream_fully_consumed(redis, stream_key):
            await redis.delete(stream_key, keys.epoch_meta(epoch))
            await redis.set(keys.low_watermark, epoch + 1)
            await metrics.note_stream_purged()
            logger.info(
                "Cleaned up epoch stream %s; low_watermark -> %d",
                stream_key,
                epoch + 1,
            )
        else:
            break  # Later epochs are also not fully consumed, so we can stop here


async def cleanup_streams_loop(
    *,
    prefix: str,
    target_epoch: int | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Delete fully consumed per-epoch Redis streams and advance low_watermark.

    Iterates epoch streams in ascending order starting from ``low_watermark``.
    An epoch stream is deleted only when ALL consumer groups have acknowledged
    every entry.

    Designed to run as a background ``asyncio.Task`` alongside the main sync
    work.  Handles ``CancelledError`` so the caller can cancel it cleanly
    when the backfill completes.

    :param prefix: Redis key prefix of the sink whose streams to clean up.
    :param target_epoch: When provided, the loop exits once ``low_watermark``
     has advanced past this epoch (i.e. all produced streams are cleaned up).
    :param logger: Logger instance. Falls back to module-level logger if not provided.
    """
    logger = logger or logging.getLogger(__name__)
    keys = EpochStreamKeys(prefix)
    redis = Redis.from_url(settings.REDIS_URL)
    metrics = MetricsClient(redis, prefix, logger)

    try:
        await asyncio.sleep(INITIAL_DELAY_SECONDS)

        while True:
            await asyncio.sleep(WAKE_INTERVAL_SECONDS)

            boundaries = await _get_boundaries(redis, keys)
            if boundaries is None:
                continue

            low_wm, last_synced = boundaries
            await _cleanup_consumed_epochs(
                redis, keys, metrics, low_wm, last_synced, logger
            )
            await metrics.note_cleanup_pass()

            if target_epoch is not None and low_wm >= target_epoch:
                logger.info(
                    "All streams up to target epoch %d cleaned up, exiting",
                    target_epoch,
                )
                break
    except asyncio.CancelledError:
        logger.info("Cleanup loop cancelled — backfill complete")
    finally:
        await redis.aclose()
