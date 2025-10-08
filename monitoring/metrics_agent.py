import asyncio
import logging
import time
from dataclasses import dataclass

import psutil
import redis.asyncio as redis
from prefect import get_run_logger, task

from config.settings import get_redis_settings


@dataclass(slots=True, frozen=True)
class SystemMetrics:
    """Consolidated metrics for the Hecate system."""

    timestamp: float
    memory_used_gb: float
    memory_used_percent: float
    redis_stream_depths: dict[str, int]
    active_epochs: list[int]
    blocks_per_second: float
    system_load: float


class MetricsAgent:
    """
    Agent that collects system metrics in a centralized way.
    Runs independently from sync workers, providing observability without impacting performance.
    """

    def __init__(self, collection_interval: int = 30):
        self.collection_interval = collection_interval
        self.redis_client: redis.Redis | None = None
        self._last_data_stream_len: int | None = None
        self._last_check_time: float | None = None
        self.logger = logging.getLogger(__name__)

    async def collect_system_metrics(self) -> SystemMetrics:
        """Collects metrics from the entire system, including blocks per second."""
        now = time.perf_counter()
        
        # Collect each metric type independently with safe defaults
        memory_used_gb, memory_used_percent = self._collect_memory_metrics()
        stream_depths, data_stream_len = await self._collect_redis_stream_metrics()
        active_epochs = await self._collect_active_epochs()
        system_load = self._collect_system_load()
        blocks_per_second = self._calculate_blocks_per_second(data_stream_len, now)

        return SystemMetrics(
            timestamp=now,
            memory_used_gb=memory_used_gb,
            memory_used_percent=memory_used_percent,
            redis_stream_depths=stream_depths,
            active_epochs=active_epochs,
            blocks_per_second=blocks_per_second,
            system_load=system_load,
        )

    def _collect_memory_metrics(self) -> tuple[float, float]:
        """Collect memory metrics with safe defaults."""
        try:
            memory = psutil.virtual_memory()
            return (memory.total - memory.available) / (1024**3), memory.percent
        except (OSError, AttributeError):
            self.logger.warning("Failed to collect memory metrics, using defaults")
            return 0.0, 0.0

    async def _collect_redis_stream_metrics(self) -> tuple[dict[str, int], int | None]:
        """Collect Redis stream metrics with safe defaults."""
        if not self.redis_client:
            return {}, None
            
        stream_depths = {}
        for stream in ["hecate:history:data_stream", "hecate:history:event_stream"]:
            try:
                stream_depths[stream] = await self.redis_client.xlen(stream)
            except (ConnectionError, TimeoutError, OSError):
                self.logger.warning("Failed to get length for stream %s", stream)
                stream_depths[stream] = 0
        
        return stream_depths, stream_depths.get("hecate:history:data_stream")

    async def _collect_active_epochs(self) -> list[int]:
        """Collect active epochs with safe defaults."""
        if not self.redis_client:
            return []
            
        try:
            resume_map_data = await self.redis_client.hgetall("hecate:history:resume_map")
            active_epochs = []
            
            for epoch_str in resume_map_data.keys():
                if epoch_str.isdigit():
                    active_epochs.append(int(epoch_str))
                else:
                    self.logger.warning("Invalid epoch key in resume_map: %s", epoch_str)
            
            return sorted(active_epochs)
        except (ConnectionError, TimeoutError, OSError):
            self.logger.warning("Failed to collect active epochs")
            return []

    def _collect_system_load(self) -> float:
        """Collect system load with safe default."""
        try:
            return psutil.getloadavg()[0] if hasattr(psutil, "getloadavg") else 0.0
        except (OSError, AttributeError):
            return 0.0

    def _calculate_blocks_per_second(self, data_stream_len: int | None, now: float) -> float:
        """Calculate blocks per second with safe defaults."""
        if (data_stream_len is None or 
            self._last_data_stream_len is None or 
            self._last_check_time is None):
            self._last_data_stream_len = data_stream_len
            self._last_check_time = now
            return 0.0
            
        delta_blocks = data_stream_len - self._last_data_stream_len
        delta_time = now - self._last_check_time
        
        self._last_data_stream_len = data_stream_len
        self._last_check_time = now
        
        return delta_blocks / delta_time if delta_time > 0 else 0.0


@task
async def collect_and_publish_metrics() -> None:
    """Task that collects and publishes system metrics."""
    logger = get_run_logger()

    try:
        agent = MetricsAgent()
        async with redis.from_url(get_redis_settings().url) as redis_client:
            agent.redis_client = redis_client
            metrics = await agent.collect_system_metrics()
            # Log metrics
            active_epochs_str = f"[{', '.join(map(str, metrics.active_epochs))}]" if metrics.active_epochs else "[]"
            logger.info(
                "System Metrics | Memory: %.2fGB (%.1f%%) | System Load: %.2f | "
                "Streams: %s | Active Epochs: %s | Blocks/sec: %.2f",
                metrics.memory_used_gb,
                metrics.memory_used_percent,
                metrics.system_load,
                metrics.redis_stream_depths,
                active_epochs_str,
                metrics.blocks_per_second
            )
            # Publish to Redis protected by try/except for cancellation
            try:
                await _publish_metrics_to_redis(redis_client, metrics)
            except asyncio.CancelledError:
                logger.info("Metrics collection task cancelled during publish to Redis.")
                return
    except asyncio.CancelledError:
        logger.info("Metrics collection task cancelled cleanly.")
    # Can perform additional cleanup here if necessary
    except (ConnectionError, TimeoutError) as e:
        logger.error("Redis connection failed: %s", e)
    except (OSError, MemoryError) as e:
        logger.error("System resource error: %s", e)
    except (ValueError, TypeError) as e:
        logger.error("Data processing error: %s", e)
    except Exception as e:  # noqa: BLE001
        # Last resort for truly unexpected errors
        logger.error("Unexpected error in metrics collection: %s", e)


async def _publish_metrics_to_redis(redis_client: redis.Redis, metrics: SystemMetrics) -> None:
    """Publish metrics to Redis stream."""
    stream_data = {
        "timestamp": metrics.timestamp,
        "memory_gb": metrics.memory_used_gb,
        "memory_percent": metrics.memory_used_percent,
        "system_load": metrics.system_load,
        "blocks_per_second": metrics.blocks_per_second,
        "active_epochs_count": len(metrics.active_epochs),
        **{f"redis_{k}": v for k, v in metrics.redis_stream_depths.items()},
    }
    
    # Add individual active epochs (up to a reasonable limit)
    for i, epoch in enumerate(metrics.active_epochs[:10]):
        stream_data[f"active_epoch_{i}"] = epoch
    
    await redis_client.xadd("hecate:metrics:system", stream_data)
