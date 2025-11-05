import asyncio
import logging

import redis.asyncio as redis


def get_contextual_logger() -> logging.Logger:
    """
    Get the logger for the current context.
    """
    try:
        from prefect import get_run_logger

        return get_run_logger()  # type: ignore[return-value]
    except (ImportError, RuntimeError):
        # Not in Prefect context or Prefect not available
        return logging.getLogger(__name__)


logger = get_contextual_logger()


class RedisBackpressureMonitor:
    """
    Monitors Redis streams to apply backpressure on the sink output.

    This monitor periodically checks the depth of a Redis stream and pauses
    processing when the stream becomes too deep, resuming when it returns to
    `max_depth` or below.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        stream_key: str,
        max_depth: int,
        check_interval: int = 10,
        wait_interval: int = 20,
    ):
        """
        Initialize the backpressure monitor.

        Args:
            redis_client: Connected Redis client instance
            stream_key: The Redis stream key to monitor
            max_depth: Maximum allowed stream depth before pausing
            check_interval: Interval in seconds between depth checks (default 10)
            wait_interval: Interval in seconds to wait between pause checks (default 20)
        """
        self.redis = redis_client
        self.stream_key = stream_key
        self.max_depth = max_depth
        self.check_interval = check_interval
        self.wait_interval = wait_interval
        self.is_paused = False
        self._monitoring_task: asyncio.Task[None] | None = None

    async def _check_stream_depth(self) -> None:
        """
        Periodically check Redis stream depth and update the pause state.

        This method runs in a continuous loop, checking the stream depth
        at regular intervals and updating the pause state when the depth
        exceeds or falls below the configured threshold.
        """
        while True:
            try:
                stream_length = await self.redis.xlen(self.stream_key)
                if stream_length >= self.max_depth and not self.is_paused:
                    logger.warning(
                        "Redis stream '%s' depth (%d) exceeds max_depth (%d). "
                        "Pausing processing.",
                        self.stream_key,
                        stream_length,
                        self.max_depth,
                    )
                    self.is_paused = True
                elif stream_length < self.max_depth and self.is_paused:
                    logger.info(
                        "Redis stream '%s' depth (%d) is back to normal. "
                        "Resuming processing.",
                        self.stream_key,
                        stream_length,
                    )
                    self.is_paused = False

                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                logger.debug("Backpressure monitoring cancelled")
                break
            except Exception as e:  # noqa: BLE001
                # Catch-all to prevent the monitor from stopping.
                logger.exception("Unexpected error in backpressure monitor: %s", e)
                self.is_paused = True

    def start(self) -> None:
        if self._monitoring_task is None or self._monitoring_task.done():
            logger.info(
                "Starting Redis backpressure monitor for stream '%s'.", self.stream_key
            )
            self._monitoring_task = asyncio.create_task(self._check_stream_depth())
        else:
            logger.warning("Backpressure monitor is already running.")

    async def stop(self) -> None:
        if self._monitoring_task and not self._monitoring_task.done():
            logger.info("Stopping Redis backpressure monitor.")
            self._monitoring_task.cancel()
            await self._monitoring_task

        self._monitoring_task = None

    async def wait_if_paused(self) -> None:
        """
        If processing is paused, this method will block until it's resumed.
        """
        while self.is_paused:
            logger.info("Waiting %d seconds due to backpressure...", self.wait_interval)
            await asyncio.sleep(self.wait_interval)
