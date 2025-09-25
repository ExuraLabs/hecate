import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

import psutil
import redis.asyncio as redis
from client.multi_source_balancer import MultiSourceBalancer
from flows.adaptive_memory_controller import AdaptiveMemoryController

logger = logging.getLogger(__name__)


@dataclass
class SystemSnapshot:
    """A snapshot of key system and application metrics."""

    memory_used_gb: float
    memory_used_percent: float
    redis_streams: Dict[str, int] = field(default_factory=dict)
    ogmios_endpoints_status: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    blocks_per_second: float = 0.0
    # Memory controller information
    memory_limit_gb: Optional[float] = None
    memory_available_gb: Optional[float] = None
    memory_status: Optional[str] = None  # NORMAL, WARNING, CRITICAL, EMERGENCY
    memory_pressure: Optional[bool] = None


class MetricsCollector:
    """Gathers and logs system and application metrics periodically."""

    def __init__(
        self,
        redis_url: str,
        stream_keys: list[str],
        balancer: Optional[MultiSourceBalancer] = None,
        memory_controller: Optional[AdaptiveMemoryController] = None,
        interval_seconds: int = 15,
    ):
        self.redis_url = redis_url
        self.stream_keys = stream_keys
        self.balancer = balancer
        self.memory_controller = memory_controller
        self.interval = interval_seconds
        self.process = psutil.Process()
        self._redis_client: Optional[redis.Redis] = None
        self._monitoring_task: Optional[asyncio.Task] = None

        # For BPS calculation
        self._last_block_count = 0
        self._last_check_time = 0  # Will be set on first update

    async def _get_memory_usage_gb(self) -> tuple[float, float]:
        """Calculates the total memory usage for the current process and its children."""
        try:
            total_used_bytes = self.process.memory_info().rss
            children = self.process.children(recursive=True)
            for child in children:
                try:
                    total_used_bytes += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            total_gb = psutil.virtual_memory().total / (1024**3)
            used_gb = total_used_bytes / (1024**3)
            used_percent = (used_gb / total_gb) * 100
            return used_gb, used_percent
        except Exception as e:
            logger.error(f"Failed to get memory usage: {e}")
            return 0.0, 0.0

    async def _get_redis_stream_depths(self) -> Dict[str, int]:
        """Gets the length of specified Redis streams."""
        if not self._redis_client:
            return {}
        depths = {}
        try:
            for key in self.stream_keys:
                depths[key] = await self._redis_client.xlen(key)
        except redis.RedisError as e:
            logger.error(f"Failed to get Redis stream depths: {e}")
        return depths

    def _get_ogmios_status(self) -> Dict[str, Dict[str, Any]]:
        """Gets the status of Ogmios endpoints from the balancer."""
        if not self.balancer:
            logger.debug("No balancer available for ogmios status")
            return {}
        
        if not hasattr(self.balancer, 'endpoints') or not self.balancer.endpoints:
            logger.debug("No endpoints found in balancer")
            return {}
            
        try:
            status = {
                str(ep.url): {
                    "is_healthy": ep.is_healthy,
                    "latency_ms": f"{ep.latency_ms:.2f}",
                    "weight": ep.weight,
                }
                for ep in self.balancer.endpoints
            }
            logger.debug(f"Collected ogmios status for {len(status)} endpoints")
            return status
        except Exception as e:
            logger.error(f"Error collecting ogmios status: {e}")
            return {}

    def update_block_count(self, blocks_processed: int) -> None:
        """Update the count of blocks processed for BPS calculation."""
        self._last_block_count += blocks_processed
        # Update timing immediately when blocks are processed
        if self._last_check_time == 0:  # First time
            self._last_check_time = time.time()

    def _calculate_blocks_per_second(self) -> float:
        """Calculate blocks per second based on recent processing."""
        try:
            current_time = time.time()
            
            # If no blocks have been processed yet, return 0
            if self._last_check_time == 0 or self._last_block_count == 0:
                return 0.0
                
            time_elapsed = current_time - self._last_check_time
            
            # If no time has elapsed, return 0
            if time_elapsed <= 0:
                return 0.0
            
            # Calculate BPS based on current counts
            bps = self._last_block_count / time_elapsed
            
            # Debug logging
            logger.debug(f"BPS calculation: {self._last_block_count} blocks in {time_elapsed:.2f}s = {bps:.2f} BPS")
            
            # Reset counters after calculation for next measurement window
            self._last_block_count = 0
            self._last_check_time = current_time
            
            return bps
        except Exception as e:
            logger.error(f"Error calculating BPS: {e}")
            return 0.0

    async def collect_snapshot(self) -> SystemSnapshot:
        """Collects a single snapshot of all metrics."""
        mem_used_gb, mem_used_percent = await self._get_memory_usage_gb()
        redis_depths = await self._get_redis_stream_depths()
        ogmios_status = self._get_ogmios_status()
        bps = self._calculate_blocks_per_second()

        # Get memory controller information if available
        memory_controller_info = {}
        if self.memory_controller:
            memory_controller_info = self.memory_controller.get_snapshot_info()

        snapshot = SystemSnapshot(
            memory_used_gb=mem_used_gb,
            memory_used_percent=mem_used_percent,
            redis_streams=redis_depths,
            ogmios_endpoints_status=ogmios_status,
            blocks_per_second=bps,
            memory_limit_gb=memory_controller_info.get("memory_limit_gb"),
            memory_available_gb=memory_controller_info.get("memory_available_gb"),
            memory_status=memory_controller_info.get("memory_status"),
            memory_pressure=memory_controller_info.get("memory_pressure"),
        )
        
        logger.debug(f"Collected metrics snapshot: {snapshot}")
        return snapshot

    async def _monitor_loop(self):
        """The main loop that periodically collects and logs metrics."""
        while True:
            try:
                snapshot = await self.collect_snapshot()
                
                # Format ogmios status for better readability
                ogmios_status_summary = "No endpoints"
                if snapshot.ogmios_endpoints_status:
                    healthy_count = sum(1 for status in snapshot.ogmios_endpoints_status.values() if status.get("is_healthy", False))
                    total_count = len(snapshot.ogmios_endpoints_status)
                    ogmios_status_summary = f"{healthy_count}/{total_count} healthy"
                
                # Format memory info using memory controller if available
                base_memory_info = f"{snapshot.memory_used_gb:.2f}GB ({snapshot.memory_used_percent:.1f}%)"
                if self.memory_controller:
                    memory_info = self.memory_controller.format_memory_info(base_memory_info)
                else:
                    memory_info = base_memory_info
                
                logger.info(
                    f"Monitoring Snapshot - "
                    f"Memory: {memory_info} | "
                    f"Redis Streams: {snapshot.redis_streams} | "
                    f"Ogmios: {ogmios_status_summary} | "
                    f"BPS: {snapshot.blocks_per_second:.2f}"
                )
            except Exception as e:
                logger.exception(f"Error in monitoring loop: {e}")

            await asyncio.sleep(self.interval)

    async def start(self):
        """Starts the monitoring background task."""
        if self._monitoring_task and not self._monitoring_task.done():
            logger.warning("Metrics collector is already running.")
            return

        try:
            self._redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self._redis_client.ping()
            logger.info("Metrics collector connected to Redis.")
        except redis.RedisError as e:
            logger.error(f"Metrics collector failed to connect to Redis: {e}")
            self._redis_client = None

        self._monitoring_task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Metrics collector started with {self.interval}s interval.")

    async def stop(self):
        """Stops the monitoring background task."""
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("Metrics collector stopped.")

        if self._redis_client:
            await self._redis_client.close()
            logger.info("Metrics collector Redis connection closed.")
