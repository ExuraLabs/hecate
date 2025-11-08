import asyncio
from multiprocessing import cpu_count
from typing import Any

from prefect import get_run_logger, task
from redis.asyncio import Redis

from config.settings import redis_settings

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check interval
TARGET_MEMORY_GB = 4.5  # Memory threshold to trigger trim

# Safety buffer calculation based on production rate
BLOCKS_PER_EPOCH = 21_600
BATCH_SIZE = 1_000
MESSAGES_PER_EPOCH = BLOCKS_PER_EPOCH // BATCH_SIZE
CONCURRENT_WORKERS = cpu_count()
MESSAGES_IN_FLIGHT = MESSAGES_PER_EPOCH * CONCURRENT_WORKERS
SAFETY_BUFFER_MESSAGES = MESSAGES_IN_FLIGHT * 2


def _compute_low_watermark(groups_info: list[dict[bytes, Any]]) -> str | None:
    """Return minimum last-delivered-id across consumer groups."""
    logger = get_run_logger()
    
    if not groups_info:
        return None

    # Check if any group hasn't started consuming
    for group in groups_info:
        if group[b"last-delivered-id"] == b"0-0":
            logger.warning(
                "Consumer group '%s' has not consumed any messages (last-delivered-id=0-0). "
                "Skipping trim to prevent data loss.",
                group[b"name"].decode("utf-8")
            )
            return None

    # Extract last-delivered-id from each group
    delivered_ids = [
        group[b"last-delivered-id"].decode("utf-8")
        for group in groups_info
    ]

    # Low watermark = minimum timestamp among all groups
    min_timestamp = min(int(sid.split("-")[0]) for sid in delivered_ids)

    return f"{min_timestamp}-0"


async def _find_trim_target(
    redis: Redis, stream_key: str, watermark_id: str, safety_buffer_messages: int
) -> str | None:
    """
    Estimate trim target ID using arithmetic approximation.
    
    Uses XINFO STREAM to get first/last entry timestamps and XLEN for count,
    then interpolates position and reverse-calculates timestamp.
    """
    # Get stream metadata
    info = await redis.xinfo_stream(stream_key)
    stream_length = info["length"]

    if stream_length == 0:
        return None

    # Extract first/last entry metadata
    first_entry = info["first-entry"]
    last_entry = info["last-entry"]

    if not first_entry or not last_entry:
        return None

    # Parse timestamps from stream IDs (format: "timestamp-sequence")
    first_ts = int(first_entry[0].decode("utf-8").split("-")[0])
    last_ts = int(last_entry[0].decode("utf-8").split("-")[0])
    watermark_ts = int(watermark_id.split("-")[0])

    # Interpolate watermark position in stream
    if last_ts == first_ts:
        # All messages have same timestamp - assume uniform distribution
        watermark_position = stream_length // 2
    else:
        # Linear interpolation
        watermark_position = int(
            ((watermark_ts - first_ts) / (last_ts - first_ts)) * stream_length
        )

    # Calculate trim position (safety buffer messages before watermark)
    trim_position = watermark_position - safety_buffer_messages

    if trim_position <= 0:
        return None  # Not enough messages to trim safely

    # Reverse interpolation
    if last_ts == first_ts:
        trim_ts = first_ts
    else:
        trim_ts = int(first_ts + (trim_position / stream_length) * (last_ts - first_ts))

    return f"{trim_ts}-0"


@task(name="cleanup_redis_streams")
async def cleanup_redis_streams_task() -> None:
    """
    Trim Redis Stream when memory exceeds threshold, maintaining safety buffer
    of messages from consumer group watermark.
    """
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
            watermark_id = _compute_low_watermark(groups_info)

            if watermark_id is None:
                continue  # No consumer groups or no consumption

            # 4. Find trim target by counting messages backwards from watermark
            trim_target_id = await _find_trim_target(
                redis, stream_key, watermark_id, SAFETY_BUFFER_MESSAGES
            )

            if trim_target_id is None:
                continue  # Could not find valid trim target

            # 5. Execute XTRIM with approximate mode
            await redis.xtrim(
                stream_key,
                minid=trim_target_id,
                approximate=True,
                ref_policy="ACKED"
            )
    except asyncio.CancelledError:
        pass  # Expected when flow completes
    finally:
        await redis.close()