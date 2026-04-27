import asyncio
from logging import Logger

from redis.asyncio import Redis

from config.settings import redis_settings
from sinks.metrics import MetricsClient, epoch_meta_key
from sinks.redis import is_stream_fully_consumed

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check interval


class RedisKeys:
    """Encapsulates Redis key naming for epoch streams."""

    _PREFIX = "hecate:history:"

    @classmethod
    def low_watermark(cls) -> str:
        return f"{cls._PREFIX}low_watermark"

    @classmethod
    def last_synced_epoch(cls) -> str:
        return f"{cls._PREFIX}last_synced_epoch"

    @classmethod
    def epoch_stream(cls, epoch: int) -> str:
        return f"{cls._PREFIX}epoch:{epoch}"

    @classmethod
    def epoch_meta(cls, epoch: int) -> str:
        return epoch_meta_key(cls._PREFIX, epoch)


async def _is_epoch_fully_consumed(
    redis: Redis,
    stream_key: str,
    logger: Logger,
) -> bool:
    """Delegate to the shared ``is_stream_fully_consumed`` helper.

    The ``logger`` parameter is kept for call-site compatibility.
    """
    return await is_stream_fully_consumed(redis, stream_key)


async def _get_boundaries(redis: Redis) -> tuple[int, int] | None:
    """Read low watermark and last synced epoch from Redis.

    Returns:
        Tuple of (low_watermark, last_synced_epoch) or None if not available.
    """
    raw_low = await redis.get(RedisKeys.low_watermark())
    raw_synced = await redis.get(RedisKeys.last_synced_epoch())

    if raw_low is None or raw_synced is None:
        return None

    return int(raw_low), int(raw_synced)


async def _cleanup_consumed_epochs(
    redis: Redis,
    metrics: MetricsClient,
    low_wm: int,
    last_synced: int,
    logger: Logger,
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
        stream_key = RedisKeys.epoch_stream(epoch)

        if not await redis.exists(stream_key):
            await redis.set(RedisKeys.low_watermark(), epoch + 1)
            logger.debug(
                "Epoch stream %s already removed; low_watermark -> %d",
                stream_key,
                epoch + 1,
            )
            continue

        if await _is_epoch_fully_consumed(redis, stream_key, logger):
            await redis.delete(stream_key, RedisKeys.epoch_meta(epoch))
            await redis.set(RedisKeys.low_watermark(), epoch + 1)
            await metrics.note_stream_purged()
            logger.info(
                "Cleaned up epoch stream %s; low_watermark -> %d",
                stream_key,
                epoch + 1,
            )
        else:
            break  # Later epochs are also not fully consumed, so we can stop here


async def cleanup_streams_loop(
    target_epoch: int | None = None,
    logger: Logger | None = None,
) -> None:
    """Delete fully consumed per-epoch Redis streams and advance low_watermark.

    Iterates epoch streams in ascending order starting from ``low_watermark``.
    An epoch stream is deleted only when ALL consumer groups have acknowledged
    every entry.

    Designed to run as a background ``asyncio.Task`` alongside the main sync
    work.  Handles ``CancelledError`` so the caller can cancel it cleanly
    when the flow completes.

    :param target_epoch: When provided, the loop exits once ``low_watermark``
     has advanced past this epoch (i.e. all produced streams are cleaned up).
    :param logger: Logger instance. Falls back to module-level logger if not provided.
    """
    import logging

    logger = logger or logging.getLogger(__name__)
    redis = Redis.from_url(redis_settings.url)
    metrics = MetricsClient(redis, RedisKeys._PREFIX, logger)

    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    try:
        while True:
            await asyncio.sleep(WAKE_INTERVAL_SECONDS)

            boundaries = await _get_boundaries(redis)
            if boundaries is None:
                continue

            low_wm, last_synced = boundaries
            await _cleanup_consumed_epochs(redis, metrics, low_wm, last_synced, logger)
            await metrics.note_cleanup_pass()

            if target_epoch is not None and low_wm >= target_epoch:
                logger.info(
                    "All streams up to target epoch %d cleaned up, exiting",
                    target_epoch,
                )
                break
    except asyncio.CancelledError:
        logger.info("Cleanup loop cancelled — flow complete")
    finally:
        await redis.close()
