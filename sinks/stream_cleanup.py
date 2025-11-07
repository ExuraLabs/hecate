import asyncio
from typing import Any

from prefect import task
from redis.asyncio import Redis

from config.settings import redis_settings

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check every n sec
TARGET_MEMORY_GB = 4.5  # Trim when stream exceeds certain GBs
SAFETY_MARGIN_EPOCHS = 8  # Keep last n epochs as buffer


def _compute_low_watermark(groups_info: list[dict[bytes, Any]]) -> str | None:
    """
    Compute low watermark (minimum last-delivered-id) across all consumer groups.
    Returns None if no groups exist or none have consumed messages.
    """
    if not groups_info:
        return None

    # Extract last-delivered-id from each group
    delivered_ids = [
        group[b"last-delivered-id"].decode("utf-8")
        for group in groups_info
        if group[b"last-delivered-id"] != b"0-0"  # Ignore groups without consumption
    ]

    if not delivered_ids:
        return None

    # Low watermark = minimum timestamp among all groups
    min_timestamp = min(int(sid.split("-")[0]) for sid in delivered_ids)

    return f"{min_timestamp}-0"


async def _find_trim_target(
    redis: Redis, stream_key: str, watermark_id: str, safety_margin: int
) -> tuple[int | None, str | None]:
    """
    Scan backwards from watermark to extract epoch and find trim target in one pass.
    Returns (watermark_epoch, trim_target_id) where trim_target is the last message
    of (watermark_epoch - safety_margin).
    """
    start_id = watermark_id  # Start from watermark (most recent point)
    batch_size = 2000
    watermark_epoch: int | None = None
    safe_epoch: int | None = None
    trim_target_id: str | None = None

    while True:
        # XREVRANGE scans from max (newest) to min (oldest)
        messages = await redis.xrevrange(
            stream_key, max=start_id, min="-", count=batch_size
        )

        if not messages:
            break

        for msg_id, msg_data in messages:
            epoch_num = int(msg_data[b"epoch"])

            # First message encountered = watermark epoch
            if watermark_epoch is None:
                watermark_epoch = epoch_num
                safe_epoch = watermark_epoch - safety_margin

                # Early exit if safe_epoch is invalid
                if safe_epoch <= 0:
                    return watermark_epoch, None

            # Now search for the last message of safe_epoch
            if epoch_num == safe_epoch:
                # First match scanning backwards = last message chronologically
                if trim_target_id is None:
                    trim_target_id = msg_id.decode("utf-8")
            elif safe_epoch is not None and epoch_num < safe_epoch:
                # Went past safe_epoch (backwards), we're done
                return watermark_epoch, trim_target_id

        # Continue scanning backwards
        last_msg_id = messages[-1][0].decode("utf-8")
        start_id = f"({last_msg_id}"  # Exclusive: continue before this ID

        # If batch is not full, reached the beginning of stream
        if len(messages) < batch_size:
            return watermark_epoch, trim_target_id

    return watermark_epoch, trim_target_id


@task(name="cleanup_redis_streams")
async def cleanup_redis_streams_task() -> None:
    """
    Long-lived background task that trims Redis Streams based on memory usage
    and consumer group progress. Wakes every certain interval of seconds, trims 
    when stream exceeds a target size and maintains a number of epochs as safety 
    buffer.
    """
    redis = Redis.from_url(redis_settings.url, decode_responses=False)
    stream_key = "hecate:history:data_stream"
    target_bytes = int(TARGET_MEMORY_GB * 1024**3)

    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    try:
        while True:
            await asyncio.sleep(WAKE_INTERVAL_SECONDS)

            # 1. Check stream memory usage (returns None if stream doesn't exist)
            stream_size_bytes = await redis.memory_usage(stream_key)

            if stream_size_bytes is None or stream_size_bytes < target_bytes:
                # Stream doesn't exist or is below threshold
                continue

            # 2. Query consumer groups to get low watermark
            groups_info = await redis.execute_command("XINFO", "GROUPS", stream_key)

            # 3. Calculate low watermark
            watermark_id = _compute_low_watermark(groups_info)

            if watermark_id is None:
                # No consumer groups or no consumption
                continue

            # 4. Get watermark epoch and trim target in single optimized scan
            watermark_epoch, trim_target_id = await _find_trim_target(
                redis, stream_key, watermark_id, SAFETY_MARGIN_EPOCHS
            )

            if watermark_epoch is None or trim_target_id is None:
                # Could not determine epoch or no valid trim target
                continue

            # 5. Execute XTRIM with approximate mode (100x faster)
            await redis.execute_command(
                "XTRIM",
                stream_key,
                "MINID",
                "~",  # Approximate mode
                trim_target_id,
            )
    except asyncio.CancelledError:
        pass  # Expected when flow completes
    finally:
        await redis.close()