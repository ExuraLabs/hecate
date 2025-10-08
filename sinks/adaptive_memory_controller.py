import asyncio
import logging
import time
from dataclasses import dataclass

import psutil
from pydantic import BaseModel

from config.settings import get_memory_settings
from config.settings import get_batch_settings


def _get_logger() -> logging.Logger:
    """Get the appropriate logger for the current context."""
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except (ImportError, RuntimeError):
        return logging.getLogger(__name__)


class AdaptiveMemoryConfig(BaseModel):
    """Configuration for the Adaptive Memory Controller."""

    memory_limit_gb: float = 4.0
    warning_threshold: float = 0.75
    critical_threshold: float = 0.85
    emergency_threshold: float = 0.90
    check_interval_seconds: int


@dataclass(slots=True, frozen=True)
class MemoryState:
    """Represents the current memory state of the system."""

    total_gb: float
    available_gb: float
    used_gb: float
    used_percent: float


@dataclass(slots=True, frozen=True)
class MemoryResponse:
    """Consolidated response from memory controller operations."""

    state: MemoryState
    should_reduce_batch: bool
    should_pause: bool
    optimal_batch_size: int
    memory_status: str
    requires_action: bool


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
        self._last_memory_response: MemoryResponse | None = None
        self.min_batch_size = get_batch_settings().min_size
        self.max_batch_size = get_batch_settings().max_size
        self.pause_interval_seconds = self.config.check_interval_seconds

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

        self.logger = _get_logger()
        self.logger.info(
            "AdaptiveMemoryController initialized with limit: "
            f"{self.config.memory_limit_gb:.2f} GB"
        )
        self.logger.debug(
            f"  - Warning threshold:  {self.warning_limit_gb:.2f} GB "
            f"({self.config.warning_threshold:.0%})"
        )
        self.logger.debug(
            f"  - Critical threshold: {self.critical_limit_gb:.2f} GB "
            f"({self.config.critical_threshold:.0%})"
        )
        self.logger.debug(
            f"  - Emergency threshold: {self.emergency_limit_gb:.2f} GB "
            f"({self.config.emergency_threshold:.0%})"
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
            total_gb=self.config.memory_limit_gb,
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

    def check_memory_and_adapt(
        self, epoch: int, original_batch_size: int, current_batch_size: int
    ) -> MemoryResponse | None:
        """
        Single unified method that checks memory state and provides all adaptation decisions.
        Only performs actual memory checks according to check_interval_seconds.

        :param epoch: Current epoch being processed (for logging)
        :param original_batch_size: The original/maximum batch size
        :param current_batch_size: The current batch size being used
        :return: MemoryResponse with all decision data, or None if no check was performed
        """
        now = time.time()
        time_since_last_check = now - self._last_check_time

        should_check_memory = (
            self._last_memory_response is None
            or time_since_last_check >= self.config.check_interval_seconds
        )

        if not should_check_memory:
            return None

        self._last_check_time = now
        state = self.get_memory_state(force_refresh=True)

        should_reduce_batch = state.used_gb >= self.critical_limit_gb
        should_pause = state.used_gb >= self.emergency_limit_gb

        if state.used_gb >= self.emergency_limit_gb:
            memory_status = "EMERGENCY"
        elif state.used_gb >= self.critical_limit_gb:
            memory_status = "CRITICAL"
        elif state.used_gb >= self.warning_limit_gb:
            memory_status = "WARNING"
        else:
            memory_status = "NORMAL"

        if should_reduce_batch:
            optimal_batch_size = max(self.min_batch_size, current_batch_size // 2)
            if optimal_batch_size != current_batch_size:
                self.logger.info(
                    "High memory pressure detected, reducing batch size from "
                    f"{current_batch_size} to {optimal_batch_size}"
                )
        elif current_batch_size < original_batch_size:
            optimal_batch_size = min(self.max_batch_size, current_batch_size + 100)
            if optimal_batch_size != current_batch_size:
                self.logger.debug(
                    "Memory pressure normal, increasing batch size from "
                    f"{current_batch_size} to {optimal_batch_size}"
                )
        else:
            optimal_batch_size = current_batch_size

        if should_pause:
            self.logger.warning(
                f"Memory emergency threshold reached during epoch {epoch} "
                f"({state.used_gb:.2f}GB/{state.total_gb:.2f}GB = {state.used_percent:.1%}), "
                f"pausing processing for memory recovery"
            )

        memory_response = MemoryResponse(
            state=state,
            should_reduce_batch=should_reduce_batch,
            should_pause=should_pause,
            optimal_batch_size=optimal_batch_size,
            memory_status=memory_status,
            requires_action=should_reduce_batch or should_pause,
        )
        self._last_memory_response = memory_response

        return memory_response

    async def handle_memory_management(
        self, epoch: int, original_batch_size: int, current_batch_size: int
    ) -> int:
        """
        High-level method that handles all memory management concerns.
        This encapsulates the full memory management workflow and maintains
        single responsibility principle in the calling code.

        :param epoch: Current epoch being processed
        :param original_batch_size: The original/maximum batch size
        :param current_batch_size: Current batch size being used
        :return: The optimal batch size to use (may be unchanged)
        """
        memory_response = self.check_memory_and_adapt(
            epoch, original_batch_size, current_batch_size
        )
        if memory_response is None:
            return current_batch_size

        await self.pause_processing_if_needed(epoch)

        self.logger.debug(
            f"Memory check performed: {memory_response.memory_status}, batch size: "
            f"{memory_response.optimal_batch_size}"
        )

        return memory_response.optimal_batch_size

    async def pause_processing_if_needed(self, epoch: int | None = None) -> None:
        """
        Pause processing when memory pressure requires it.
        Uses the last memory check result

        :param epoch: Optional epoch number for logging context
        """
        if (
            self._last_memory_response is None
            or not self._last_memory_response.should_pause
        ):
            return

        state = self._last_memory_response.state

        self.logger.warning(
            f"Memory emergency threshold reached during epoch {epoch or '-'} "
            f"({state.used_gb:.2f}GB/{state.total_gb:.2f}GB = {state.used_percent:.1%}), "
            f"pausing processing for memory recovery"
        )

        await asyncio.sleep(self.pause_interval_seconds)

    def should_reduce_batch_size(self) -> bool:
        """
        Returns True if memory usage is above the critical threshold.
        """
        return getattr(self._last_memory_response, "should_reduce_batch", False)

    def should_pause_processing(self) -> bool:
        """
        Returns True if memory usage is above the emergency threshold.
        """
        if self._last_memory_response is None:
            return False
        return self._last_memory_response.should_pause

    def get_optimal_batch_size(
        self, original_batch_size: int, current_batch_size: int
    ) -> int:
        """
        Returns the optimal batch size based on current memory pressure.
        Encapsulates all batch size adaptation logic.

        :param original_batch_size: The original/maximum batch size
        :param current_batch_size: The current batch size being used
        :return: The optimal batch size to use
        """
        if self._last_memory_response is None:
            return current_batch_size

        response = self._last_memory_response

        if response.should_reduce_batch:
            optimal_batch_size = max(self.min_batch_size, current_batch_size // 2)
        elif current_batch_size < original_batch_size:
            optimal_batch_size = min(self.max_batch_size, current_batch_size + 100)
        else:
            optimal_batch_size = current_batch_size

        return optimal_batch_size

    def get_snapshot_info(self) -> dict:
        """Get memory controller information for system snapshots."""
        if self._last_memory_response is None:
            return {
                "memory_limit_gb": self.config.memory_limit_gb,
                "memory_available_gb": None,
                "memory_status": "NO_CHECK",
                "memory_pressure": False,
            }

        response = self._last_memory_response
        return {
            "memory_limit_gb": response.state.total_gb,
            "memory_available_gb": response.state.available_gb,
            "memory_status": response.memory_status,
            "memory_pressure": response.requires_action,
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
        except (KeyError, TypeError):
            return base_memory_info
