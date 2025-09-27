import asyncio
import logging
from typing import Optional

import redis.asyncio as redis
from pydantic import BaseModel

from config.settings import get_redis_settings


logger = logging.getLogger(__name__)


class RedisBackpressureConfig(BaseModel):
    """Configuration for Redis backpressure monitoring."""

    max_depth: int = 10_000
    check_interval: int


class RedisBackpressureMonitor:
    """
    Monitors Redis streams to apply backpressure and prevent OOM issues.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        stream_key: str,
        config: Optional[RedisBackpressureConfig] = None,
    ):
        self.redis = redis_client
        self.stream_key = stream_key
        
        if config is None:
            redis_settings = get_redis_settings()
            config = RedisBackpressureConfig(
                max_depth=redis_settings.max_stream_depth,
                check_interval=redis_settings.check_interval,
            )
        
        self.config = config
        self._is_paused = False
        self._monitoring_task: Optional[asyncio.Task] = None

    async def _check_stream_depth(self) -> None:
        """Periodically checks the Redis stream depth and updates the pause state."""
        while True:
            try:
                stream_length = await self.redis.xlen(self.stream_key)
                if stream_length >= self.config.max_depth:
                    if not self._is_paused:
                        logger.warning(
                            f"Redis stream '{self.stream_key}' depth ({stream_length}) "
                            f"exceeds max_depth ({self.config.max_depth}). "
                            "Pausing processing."
                        )
                        self._is_paused = True
                elif self._is_paused:
                    logger.info(
                        f"Redis stream '{self.stream_key}' depth ({stream_length}) "
                        "is back to normal. Resuming processing."
                    )
                    self._is_paused = False
            except redis.RedisError as e:
                logger.error(f"Error checking Redis stream depth: {e}")
                # In case of Redis error, we pause to be safe
                self._is_paused = True
            except Exception as e:
                logger.exception(f"Unexpected error in backpressure monitor: {e}")
                self._is_paused = True

            await asyncio.sleep(self.config.check_interval)

    def start(self) -> None:
        """Starts the background monitoring task."""
        if self._monitoring_task is None or self._monitoring_task.done():
            logger.info(
                f"Starting Redis backpressure monitor for stream '{self.stream_key}'."
            )
            self._monitoring_task = asyncio.create_task(self._check_stream_depth())
        else:
            logger.warning("Backpressure monitor is already running.")

    async def stop(self) -> None:
        """Stops the background monitoring task."""
        if self._monitoring_task and not self._monitoring_task.done():
            logger.info("Stopping Redis backpressure monitor.")
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        self._monitoring_task = None

    async def wait_if_paused(self) -> None:
        """
        If processing is paused, this method will block until it's resumed.
        """
        while self._is_paused:
            await asyncio.sleep(1)

    @property
    def is_paused(self) -> bool:
        """Returns True if processing should be paused, False otherwise."""
        return self._is_paused
