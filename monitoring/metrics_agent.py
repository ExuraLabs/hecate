import logging
import time
import threading
from dataclasses import dataclass

import psutil
import redis.asyncio as redis

from config.settings import get_redis_settings


# Common exception types for Redis and system operations
REDIS_ERRORS = (ConnectionError, TimeoutError, OSError)
SYSTEM_ERRORS = (OSError, AttributeError)
ALL_COMMON_ERRORS = (ConnectionError, TimeoutError, RuntimeError, OSError)


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
    
    Implemented as singleton to maintain state (last measurements) between calls
    for accurate blocks/second calculation.
    """
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Thread-safe singleton implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Only initialize once, even if __init__ is called multiple times
        # Use lock to prevent race condition during initialization
        with self._lock:
            if hasattr(self, '_initialized'):
                return
                
            self.redis_client: redis.Redis | None = None
            self._last_data_stream_len: int | None = None
            self._last_check_time: float | None = None
            self.logger = logging.getLogger(__name__)
            self._initialized = True

    @classmethod
    def get_instance(cls) -> "MetricsAgent":
        """Get the singleton instance of MetricsAgent."""
        return cls()

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            cls._instance = None

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
        except SYSTEM_ERRORS:
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
            except REDIS_ERRORS:
                self.logger.warning("Failed to get length for stream %s", stream)
                stream_depths[stream] = 0
        
        return stream_depths, stream_depths.get("hecate:history:data_stream")

    async def _collect_active_epochs(self) -> list[int]:
        """
        Collect active epochs with safe defaults.
        
        Looks for epochs that are currently being processed by checking:
        1. resume_map: epochs with active resume positions
        2. ready_set: epochs completed but awaiting sequential commit
        3. recent data_stream entries: epochs that have sent batches recently
        """
        if not self.redis_client:
            return []
            
        try:
            active_epochs = set()
            
            # Check resume_map for epochs with active resume positions
            resume_map_data = await self.redis_client.hgetall("hecate:history:resume_map")
            for epoch_bytes in resume_map_data.keys():
                epoch_str = epoch_bytes.decode() if isinstance(epoch_bytes, bytes) else str(epoch_bytes)
                if epoch_str.isdigit():
                    active_epochs.add(int(epoch_str))
                else:
                    self.logger.warning("Invalid epoch key in resume_map: %s", epoch_str)
            
            # Check ready_set for epochs awaiting sequential commit
            try:
                ready_set_data = await self.redis_client.smembers("hecate:history:ready_set")
                for epoch_bytes in ready_set_data:
                    epoch_str = epoch_bytes.decode() if isinstance(epoch_bytes, bytes) else str(epoch_bytes)
                    if epoch_str.isdigit():
                        active_epochs.add(int(epoch_str))
            except REDIS_ERRORS:
                self.logger.debug("Could not read ready_set for active epochs")
            
            # Check recent entries in data_stream for recently active epochs
            try:
                recent_entries = await self.redis_client.xrevrange(
                    "hecate:history:data_stream", 
                    count=50  # Look at last 50 entries
                )
                for entry_id, fields in recent_entries:
                    if b'epoch' in fields:
                        epoch_str = fields[b'epoch'].decode()
                        if epoch_str.isdigit():
                            active_epochs.add(int(epoch_str))
            except REDIS_ERRORS:
                self.logger.debug("Could not read data_stream for active epochs")
            
            return sorted(list(active_epochs))
            
        except REDIS_ERRORS:
            self.logger.warning("Failed to collect active epochs")
            return []

    def _collect_system_load(self) -> float:
        """Collect system load with safe default."""
        try:
            return psutil.getloadavg()[0] if hasattr(psutil, "getloadavg") else 0.0
        except SYSTEM_ERRORS:
            return 0.0

    def _calculate_blocks_per_second(self, data_stream_len: int | None, now: float) -> float:
        """
        Calculate blocks per second with safe defaults.
        
        This method maintains state between calls to provide accurate rate calculation.
        Since the MetricsAgent is now a singleton, the state persists across metric collections.
        """
        if data_stream_len is None:
            # If we can't get stream length, reset state and return 0
            self._last_data_stream_len = None
            self._last_check_time = now
            return 0.0
            
        if (self._last_data_stream_len is None or self._last_check_time is None):
            # First measurement - initialize state and return 0
            self._last_data_stream_len = data_stream_len
            self._last_check_time = now
            return 0.0
            
        delta_blocks = data_stream_len - self._last_data_stream_len
        delta_time = now - self._last_check_time
        
        self._last_data_stream_len = data_stream_len
        self._last_check_time = now
        
        # Avoid division by zero and handle edge cases
        if delta_time <= 0:
            return 0.0
            
        blocks_per_second = delta_blocks / delta_time
        
        # Log debug info for troubleshooting
        if delta_blocks > 0:
            self.logger.debug(
                "Blocks/sec calculation: %d blocks in %.2fs = %.2f blocks/sec",
                delta_blocks, delta_time, blocks_per_second
            )
        
        return max(0.0, blocks_per_second)  # Ensure non-negative


async def collect_and_publish_metrics(agent: MetricsAgent | None = None) -> None:
    """
    Collect and publish system metrics once (called periodically by the flow).
    
    Args:
        agent: Optional MetricsAgent instance. If None, gets the singleton instance.
    """
    from prefect import get_run_logger
    logger = get_run_logger()

    if agent is None:
        agent = MetricsAgent.get_instance()
        
    try:
        async with redis.from_url(get_redis_settings().url) as redis_client:
            agent.redis_client = redis_client
            metrics = await agent.collect_system_metrics()
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
            await _publish_metrics_to_redis(redis_client, metrics)
    except (ConnectionError, TimeoutError) as e:
        logger.error("Redis connection failed: %s", e)
    except (OSError, MemoryError) as e:
        logger.error("System resource error: %s", e)
    except (ValueError, TypeError) as e:
        logger.error("Data processing error: %s", e)
    except Exception as e:  # noqa: BLE001
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
