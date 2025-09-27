import logging
import time
from dataclasses import dataclass

import psutil
from pydantic import BaseModel

from config.settings import get_memory_settings


def _get_logger():
    """Get the appropriate logger for the current context."""
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except (ImportError, RuntimeError):
        # Fallback to standard logging if Prefect is not available or no run context
        return logging.getLogger(__name__)


class AdaptiveMemoryConfig(BaseModel):
    """Configuration for the Adaptive Memory Controller."""

    memory_limit_gb: float = 16.0
    warning_threshold: float = 0.75
    critical_threshold: float = 0.85
    emergency_threshold: float = 0.90
    check_interval_seconds: int


@dataclass
class MemoryState:
    """Represents the current memory state of the system."""

    total_gb: float
    available_gb: float
    used_gb: float
    used_percent: float


class AdaptiveMemoryController:
    """
    Monitors system memory and provides guidance on adapting application
    behavior (e.g., adjusting batch sizes, pausing) to stay within limits.
    """

    def __init__(self, config: AdaptiveMemoryConfig | None = None):
        if config is None:
            memory_settings = get_memory_settings()
            config = AdaptiveMemoryConfig(**memory_settings.model_dump())

        self.config = config
        self.process = psutil.Process()
        self._last_check_time: float = 0
        self._current_state: MemoryState | None = None

        # Calculate absolute thresholds in GB
        self.warning_limit_gb = (
            self.config.memory_limit_gb * self.config.warning_threshold
        )
        self.critical_limit_gb = (
            self.config.memory_limit_gb * self.config.critical_threshold
        )
        self.emergency_limit_gb = (
            self.config.memory_limit_gb * self.config.emergency_threshold
        )

        logger = _get_logger()
        logger.info(
            f"AdaptiveMemoryController initialized with limit: {self.config.memory_limit_gb:.2f} GB"
        )
        logger.info(
            f"  - Warning threshold:  {self.warning_limit_gb:.2f} GB ({self.config.warning_threshold:.0%})"
        )
        logger.info(
            f"  - Critical threshold: {self.critical_limit_gb:.2f} GB ({self.config.critical_threshold:.0%})"
        )
        logger.info(
            f"  - Emergency threshold: {self.emergency_limit_gb:.2f} GB ({self.config.emergency_threshold:.0%})"
        )

    def _get_current_memory_usage(self) -> MemoryState:
        """Gets the current memory usage of the process and its children."""
        mem_info = self.process.memory_info()
        total_used_bytes = mem_info.rss

        # Include memory from child processes
        children = self.process.children(recursive=True)
        for child in children:
            try:
                total_used_bytes += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        used_gb = total_used_bytes / (1024**3)

        return MemoryState(
            total_gb=self.config.memory_limit_gb,  # This is the configured limit, not system total
            available_gb=self.config.memory_limit_gb - used_gb,
            used_gb=used_gb,
            used_percent=used_gb / self.config.memory_limit_gb,
        )

    def get_memory_state(self, force_refresh: bool = False) -> MemoryState:
        """
        Returns the current memory state, using a cached value if checked recently.
        """
        now = time.time()
        if (
            force_refresh
            or not self._current_state
            or (now - self._last_check_time) > self.config.check_interval_seconds
        ):
            self._current_state = self._get_current_memory_usage()
            self._last_check_time = now

        return self._current_state

    def should_reduce_batch_size(self) -> bool:
        """
        Returns True if memory usage is above the critical threshold.
        """
        state = self.get_memory_state()
        return state.used_gb >= self.critical_limit_gb

    def should_pause_processing(self) -> bool:
        """
        Returns True if memory usage is above the emergency threshold.
        """
        state = self.get_memory_state()
        return state.used_gb >= self.emergency_limit_gb

    def get_snapshot_info(self) -> dict:
        """Get memory controller information for system snapshots."""
        try:
            state = self.get_memory_state()
            memory_pressure = (
                self.should_reduce_batch_size() or self.should_pause_processing()
            )

            # Determine status based on thresholds
            if state.used_gb >= self.emergency_limit_gb:
                memory_status = "EMERGENCY"
            elif state.used_gb >= self.critical_limit_gb:
                memory_status = "CRITICAL"
            elif state.used_gb >= self.warning_limit_gb:
                memory_status = "WARNING"
            else:
                memory_status = "NORMAL"

            return {
                "memory_limit_gb": state.total_gb,
                "memory_available_gb": state.available_gb,
                "memory_status": memory_status,
                "memory_pressure": memory_pressure,
            }
        except Exception as e:
            logger = _get_logger()
            logger.debug(f"Error collecting memory controller info: {e}")
            return {
                "memory_limit_gb": None,
                "memory_available_gb": None,
                "memory_status": None,
                "memory_pressure": None,
            }

    def format_memory_info(self, base_memory_info: str) -> str:
        """Format memory information for display, including controller status."""
        try:
            snapshot_info = self.get_snapshot_info()
            memory_info = base_memory_info

            if snapshot_info["memory_status"]:
                memory_info += f" [{snapshot_info['memory_status']}]"
            if snapshot_info["memory_pressure"]:
                memory_info += " PRESSURE!"

            return memory_info
        except Exception:
            return base_memory_info
