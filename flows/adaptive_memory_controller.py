import logging
import time
from dataclasses import dataclass

import psutil
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AdaptiveMemoryConfig(BaseModel):
    """Configuration for the Adaptive Memory Controller."""

    memory_limit_gb: float = 16.0
    warning_threshold: float = 0.75
    critical_threshold: float = 0.85
    emergency_threshold: float = 0.90
    check_interval_seconds: int = 10


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
        self.config = config or AdaptiveMemoryConfig()
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
        is_critical = state.used_gb >= self.critical_limit_gb
        if is_critical:
            logger.warning(
                f"Memory usage ({state.used_gb:.2f} GB) has passed the critical threshold "
                f"({self.critical_limit_gb:.2f} GB). Suggesting batch size reduction."
            )
        return is_critical

    def should_pause_processing(self) -> bool:
        """
        Returns True if memory usage is above the emergency threshold.
        """
        state = self.get_memory_state()
        is_emergency = state.used_gb >= self.emergency_limit_gb
        if is_emergency:
            logger.error(
                f"EMERGENCY: Memory usage ({state.used_gb:.2f} GB) has passed the emergency threshold "
                f"({self.emergency_limit_gb:.2f} GB). Suggesting immediate pause."
            )
        return is_emergency
