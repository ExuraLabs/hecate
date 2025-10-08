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

    async def collect_system_metrics(self) -> SystemMetrics:
        """Collects metrics from the entire system, including blocks per second."""
        # Memory metrics
        memory = psutil.virtual_memory()
        memory_used_gb = (memory.total - memory.available) / (1024**3)
        memory_used_percent = memory.percent
        
        # Redis metrics
        stream_depths = {}
        data_stream_len = None
        active_epochs = []
        
        if self.redis_client:
            # Stream depths
            for stream in ["hecate:history:data_stream", "hecate:history:event_stream"]:
                stream_depths[stream] = await self.redis_client.xlen(stream)
            data_stream_len = stream_depths.get("hecate:history:data_stream", None)
            
            # Active epochs from resume_map (epochs currently being processed)
            resume_map_data = await self.redis_client.hgetall("hecate:history:resume_map")
            if resume_map_data:
                active_epochs = [int(epoch) for epoch in resume_map_data.keys()]
                active_epochs.sort()  # Sort for consistent ordering
        
        # System metrics
        system_load = psutil.getloadavg()[0] if hasattr(psutil, "getloadavg") else 0.0

        # Calculate blocks per second (BPS) using stream length delta and time interval
        now = time.perf_counter()
        if not all([
                data_stream_len is not None,
                self._last_data_stream_len is not None,
                self._last_check_time is not None,
            ]):
            blocks_per_second = 0.0
        else:
            # Type ignore because mypy can't infer from the `all` check
            delta_blocks = data_stream_len - self._last_data_stream_len  # type: ignore
            delta_time = now - self._last_check_time  # type: ignore
            blocks_per_second = (
                delta_blocks / delta_time if delta_time > 0 else 0.0
            )

        self._last_data_stream_len = data_stream_len
        self._last_check_time = now

        return SystemMetrics(
            timestamp=now,
            memory_used_gb=memory_used_gb,
            memory_used_percent=memory_used_percent,
            redis_stream_depths=stream_depths,
            active_epochs=active_epochs,
            blocks_per_second=blocks_per_second,
            system_load=system_load,
        )


@task
async def collect_and_publish_metrics() -> None:
    """Task that collects and publishes system metrics."""
    agent = MetricsAgent()
    async with redis.from_url(get_redis_settings().url) as redis_client:
        agent.redis_client = redis_client
        logger = get_run_logger()
        metrics = await agent.collect_system_metrics()
        
        # Format active epochs for logging
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
        
        # Prepare metrics for Redis stream
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
        if metrics.active_epochs:
            # Limit to first 10 epochs to avoid excessive data
            for i, epoch in enumerate(metrics.active_epochs[:10]):
                stream_data[f"active_epoch_{i}"] = epoch
        
        await redis_client.xadd("hecate:metrics:system", stream_data)
