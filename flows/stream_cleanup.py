import asyncio
from logging import Logger
from typing import Any

from prefect import get_run_logger, task
from redis.asyncio import Redis

from config.settings import redis_settings

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


async def _is_epoch_fully_consumed(
    redis: Redis,
    stream_key: str,
    logger: Logger,
) -> bool:
    """Return True if all consumer groups have fully consumed the stream.

    A stream is considered fully consumed when:
    - At least one consumer group exists
    - Every group has 0 pending entries
    - Every group's ``last-delivered-id`` equals the stream's ``last-generated-id``
    """
    stream_info: dict[str, Any] = await redis.xinfo_stream(stream_key)

    last_generated_id = stream_info.get("last-generated-id", b"0-0")
    if isinstance(last_generated_id, bytes):
        last_generated_id = last_generated_id.decode()

    groups_info: list[dict[str, Any]] = await redis.xinfo_groups(stream_key)

    if not groups_info:
        return False

    for group in groups_info:
        pending = group.get("pending", 0)
        if pending > 0:
            return False

        last_delivered = group.get("last-delivered-id", b"0-0")
        if isinstance(last_delivered, bytes):
            last_delivered = last_delivered.decode()

        if last_delivered != last_generated_id:
            return False

    return True


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
    low_wm: int,
    last_synced: int,
    logger: Logger,
) -> None:
    """Iterate through epochs and delete fully consumed streams.

    Stops at the first unconsumed epoch because reads are sequential,
    meaning subsequent epochs are also not consumed yet.
    """
    for epoch in range(low_wm, last_synced + 1):
        stream_key = RedisKeys.epoch_stream(epoch)

        if await _is_epoch_fully_consumed(redis, stream_key, logger):
            await redis.delete(stream_key)
            await redis.set(RedisKeys.low_watermark(), epoch + 1)
            logger.info(
                "Cleaned up epoch stream %s; low_watermark -> %d",
                stream_key,
                epoch + 1,
            )
        else:
            break  # Later epochs are also not fully consumed, so we can stop here


@task(name="cleanup_redis_streams")
async def cleanup_redis_streams_task() -> None:
    """Delete fully consumed per-epoch Redis streams and advance low_watermark.

    Iterates epoch streams in ascending order starting from ``low_watermark``.
    An epoch stream is deleted only when ALL consumer groups have acknowledged
    every entry.
    """
    logger = get_run_logger()
    redis = Redis.from_url(redis_settings.url)

    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    try:
        while True:
            await asyncio.sleep(WAKE_INTERVAL_SECONDS)

            boundaries = await _get_boundaries(redis)
            if boundaries is None:
                continue

            low_wm, last_synced = boundaries
            await _cleanup_consumed_epochs(redis, low_wm, last_synced, logger)  # type: ignore
    except asyncio.CancelledError:
        pass  # Expected when flow completes
    finally:
        await redis.close()
