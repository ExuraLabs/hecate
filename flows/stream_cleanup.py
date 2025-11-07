import asyncio
from multiprocessing import cpu_count
from typing import Any

from prefect import task
from redis.asyncio import Redis

from config.settings import redis_settings

INITIAL_DELAY_SECONDS = 80  # Initial grace period for component startup
WAKE_INTERVAL_SECONDS = 45  # Check interval
TARGET_MEMORY_GB = 4.5  # Memory threshold to trigger trim

# Stream depth configuration
BLOCKS_PER_EPOCH = 21_600  # Cardano mainnet average
BATCH_SIZE = 1000  # Production batch size (blocks per message)
MESSAGES_PER_EPOCH = BLOCKS_PER_EPOCH // BATCH_SIZE  # 21 messages/epoch

# Safety buffer calculation (stream depth based)
CONCURRENT_WORKERS = cpu_count()
SAFETY_BUFFER_MESSAGES = CONCURRENT_WORKERS * 2


def _compute_low_watermark(groups_info: list[dict[bytes, Any]]) -> str | None:
    """Return minimum last-delivered-id across consumer groups."""
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
    redis: Redis, stream_key: str, watermark_id: str, safety_buffer_messages: int
) -> str | None:
    """Count backwards N messages from watermark and return that stream ID."""
    messages_counted = 0
    start_id = watermark_id
    batch_size = 2000  # XREVRANGE batch size (network optimization)

    while True:
        # XREVRANGE scans from max (newest) to min (oldest)
        messages = await redis.xrevrange(
            stream_key, max=start_id, min="-", count=batch_size
        )

        if not messages:
            return None  # Reached beginning without counting enough

        for msg_id, _ in messages:  # Only need stream_id, not payload
            messages_counted += 1

            if messages_counted == safety_buffer_messages:
                # Found target: this message is exactly N messages back from watermark
                return msg_id.decode("utf-8")

        # Continue scanning backwards
        last_msg_id = messages[-1][0].decode("utf-8")
        start_id = f"({last_msg_id}"  # Exclusive: continue before this ID

        if len(messages) < batch_size:
            return None  # Reached beginning without finding target


@task(name="cleanup_redis_streams")
async def cleanup_redis_streams_task() -> None:
    """
    Trim Redis Stream when memory exceeds threshold, maintaining safety buffer
    of messages from consumer group watermark.
    """
    redis = Redis.from_url(redis_settings.url, decode_responses=False)
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
            groups_info = await redis.execute_command("XINFO", "GROUPS", stream_key)

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