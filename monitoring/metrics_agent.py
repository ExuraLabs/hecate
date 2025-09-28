import asyncio
import logging
from dataclasses import dataclass
from typing import Dict

import redis.asyncio as redis
import psutil
import time
from prefect import flow, task
from config.settings import get_redis_settings

logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """Consolidated metrics for the Hecate system."""
    timestamp: float
    memory_used_gb: float
    memory_used_percent: float
    redis_stream_depths: Dict[str, int]
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
        if self.redis_client:
            for stream in ["hecate:history:data_stream", "hecate:history:event_stream"]:
                stream_depths[stream] = await self.redis_client.xlen(stream)
            data_stream_len = stream_depths.get("hecate:history:data_stream", None)
        # System metrics
        system_load = psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0.0

        # Calculate blocks per second (BPS) using stream length delta and time interval
        now = time.perf_counter()
        blocks_per_second = 0.0
        if data_stream_len is not None:
            if self._last_data_stream_len is not None and self._last_check_time is not None:
                delta_blocks = data_stream_len - self._last_data_stream_len
                delta_time = now - self._last_check_time
                if delta_time > 0:
                    blocks_per_second = delta_blocks / delta_time
            self._last_data_stream_len = data_stream_len
            self._last_check_time = now

        return SystemMetrics(
            timestamp=now,
            memory_used_gb=memory_used_gb,
            memory_used_percent=memory_used_percent,
            redis_stream_depths=stream_depths,
            active_epochs=[],  # TODO: Get from Redis
            blocks_per_second=blocks_per_second,
            system_load=system_load,
        )

@task
async def collect_and_publish_metrics() -> None:
    """Task that collects and publishes system metrics."""
    if logger.level > logging.INFO:
        logger.setLevel(logging.INFO)
    agent = MetricsAgent()
    async with redis.from_url(get_redis_settings().url) as redis_client:
        agent.redis_client = redis_client
        metrics = await agent.collect_system_metrics()
        # Publish metrics to Redis for dashboards
        log_msg = (
            f"📊 Metrics | "
            f"Memory: {metrics.memory_used_gb:.2f}GB ({metrics.memory_used_percent:.1f}%) | "
            f"System Load: {metrics.system_load:.2f} | "
            f"Streams: {metrics.redis_stream_depths} | "
            f"Active Epochs: {metrics.active_epochs} | "
            f"Blocks/sec: {metrics.blocks_per_second:.2f} | "
        )
        logger.info(log_msg)
        await redis_client.xadd(
            "hecate:metrics:system", 
            {
                "timestamp": metrics.timestamp,
                "memory_gb": metrics.memory_used_gb,
                "memory_percent": metrics.memory_used_percent,
                "system_load": metrics.system_load,
                **{f"redis_{k}": v for k, v in metrics.redis_stream_depths.items()},
            }
        )


@flow(name="metrics-agent", log_prints=True)
async def metrics_collection_flow() -> None:
    """Main flow for the metrics agent."""
    logger.info("🔍 Starting Hecate metrics collection agent")
    while True:
        try:
            await collect_and_publish_metrics()
            await asyncio.sleep(30)  # Collect every 30 seconds
        except Exception as e:
            logger.error(f"Error in metrics collection: {e}")
            await asyncio.sleep(60)  # Backoff in case of error
