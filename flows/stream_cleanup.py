import asyncio
from logging import Logger
from typing import Any

from prefect import get_run_logger, task
from redis.asyncio import Redis

from config.settings import redis_settings

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check interval
TARGET_MEMORY_GB = 4.5  # Memory threshold to trigger trim

def _compute_low_watermark(
    groups_info: list[dict[str, Any]], logger: Logger
) -> str | None:
    """Return minimum last-delivered-id across consumer groups."""
    if not groups_info:
        return None

    delivered_ids = []

    # Check if any group hasn't started consuming
    for group in groups_info:
        if group["last-delivered-id"] == "0-0":
            logger.warning(
                "Consumer group '%s' has not consumed any messages "
                "(last-delivered-id=0-0). Skipping trim to prevent data loss.",
                group["name"],
            )
            return None
        delivered_ids.append(group["last-delivered-id"])

    # Low watermark = minimum timestamp among all groups
    min_timestamp = min(int(sid.split("-")[0]) for sid in delivered_ids)

    return f"{min_timestamp}-0"


@task(name="cleanup_redis_streams")
async def cleanup_redis_streams_task() -> None:
    """
    Trim Redis Stream when memory exceeds certain threshold
    """
    logger = get_run_logger()
    redis = Redis.from_url(redis_settings.url)
    stream_key = "hecate:history:data_stream"
    target_bytes = int(TARGET_MEMORY_GB * 1024**3)

    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    try:
        while True:
            await asyncio.sleep(WAKE_INTERVAL_SECONDS)

            # 1. Check stream memory usage
            stream_size_bytes = await redis.memory_usage(stream_key)

            if stream_size_bytes is None or stream_size_bytes < target_bytes:
                continue  # Stream doesn't exist or below threshold

            # 2. Query consumer groups to get low watermark
            groups_info = await redis.xinfo_groups(stream_key)

            # 3. Calculate low watermark (minimum consumed position)
            watermark_id = _compute_low_watermark(groups_info, logger)

            if watermark_id is None:
                continue  # No consumer groups or no consumption

            # 4. Execute XTRIM with approximate mode
            await redis.xtrim(
                stream_key,
                minid=watermark_id,
                approximate=True,
                ref_policy="ACKED"
            )
    except asyncio.CancelledError:
        pass  # Expected when flow completes
    finally:
        await redis.close()
