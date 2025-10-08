"""
Memory management domain: monitoring and policy evaluation.

This module contains classes responsible for memory-related operations:
- MemoryMonitor: Collects memory usage data from the system
- MemoryPolicyEngine: Evaluates memory state and makes decisions
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

import psutil
from pydantic import BaseModel


def get_contextual_logger() -> logging.Logger:
    """
    Get the logger for the current context.
    In Prefect workflows, uses get_run_logger().
    Outside Prefect context, falls back to module logger.
    """
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except (ImportError, RuntimeError):
        return logging.getLogger(__name__)


class MemoryConfig(BaseModel):
    """Configuration for memory management."""

    memory_limit_gb: float = 4.0
    warning_threshold: float = 0.75
    critical_threshold: float = 0.85
    emergency_threshold: float = 0.90
    check_interval_seconds: int = 30


@dataclass(slots=True, frozen=True)
class MemoryState:
    """Represents the current memory state of the system."""

    total_gb: float
    available_gb: float
    used_gb: float
    used_percent: float


@dataclass(slots=True, frozen=True)
class MemoryDecision:
    """Decision result from memory policy evaluation."""

    state: MemoryState
    should_reduce_batch: bool
    should_pause: bool
    memory_status: str
    requires_action: bool


class MemoryMonitor:
    """
    Responsible ONLY for collecting memory usage data from the system.
    
    Single Responsibility: Memory data collection and caching.
    """

    def __init__(self, config: MemoryConfig):
        self.config = config
        self.process = psutil.Process()
        self._last_check_time: float = 0
        self._cached_state: MemoryState | None = None
        self.logger = get_contextual_logger()

    def get_current_memory_state(self, force_refresh: bool = False) -> MemoryState:
        """
        Get the current memory state, using cached value if recently checked.
        
        Args:
            force_refresh: If True, bypass cache and get fresh memory state
            
        Returns:
            Current memory state including usage and availability
        """
        current_time = time.time()
        time_since_last_check = current_time - self._last_check_time
        
        if (
            force_refresh
            or not self._cached_state
            or time_since_last_check > self.config.check_interval_seconds
        ):
            self._cached_state = self._collect_memory_usage()
            self._last_check_time = current_time

        return self._cached_state

    def _collect_memory_usage(self) -> MemoryState:
        """
        Collect current memory usage of the process and its children.
        
        Returns:
            MemoryState with current usage metrics
        """
        try:
            mem_info = self.process.memory_info()
            total_used_bytes = mem_info.rss

            # Include memory from child processes
            children = self.process.children(recursive=True)
            for child in children:
                try:
                    total_used_bytes += child.memory_info().rss
                except (psutil.ZombieProcess, psutil.NoSuchProcess, psutil.AccessDenied, OSError) as e:
                    self.logger.debug("Error getting memory info for child process: %s", e)
                    continue

            used_gb = total_used_bytes / (1024**3)

            return MemoryState(
                total_gb=self.config.memory_limit_gb,
                available_gb=self.config.memory_limit_gb - used_gb,
                used_gb=used_gb,
                used_percent=used_gb / self.config.memory_limit_gb,
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, MemoryError) as e:
            self.logger.error("Critical error getting memory usage: %s", e)
            # Return safe default state to prevent further errors
            return MemoryState(
                total_gb=self.config.memory_limit_gb,
                available_gb=0.0,
                used_gb=self.config.memory_limit_gb,
                used_percent=1.0,
            )


class MemoryPolicyEngine:
    """
    Responsible ONLY for evaluating memory state and making policy decisions.
    
    Single Responsibility: Memory-based decision making.
    """

    def __init__(self, config: MemoryConfig):
        self.config = config
        self.logger = get_contextual_logger()
        
        # Calculate absolute thresholds in GB
        self.warning_limit_gb = self.config.memory_limit_gb * self.config.warning_threshold
        self.critical_limit_gb = self.config.memory_limit_gb * self.config.critical_threshold
        self.emergency_limit_gb = self.config.memory_limit_gb * self.config.emergency_threshold

    def evaluate_memory_state(self, state: MemoryState, epoch: int | None = None) -> MemoryDecision:
        """
        Evaluate memory state and determine required actions.
        
        Args:
            state: Current memory state
            epoch: Optional epoch number for logging context
            
        Returns:
            MemoryDecision with recommended actions
        """
        should_reduce_batch = state.used_gb >= self.critical_limit_gb
        should_pause = state.used_gb >= self.emergency_limit_gb
        memory_status = self._determine_memory_status(state)
        
        # Log warnings for critical states
        if should_pause and epoch is not None:
            self.logger.warning(
                "Memory emergency threshold reached during epoch %s (%.2fGB/%.2fGB = %.1f%%), pausing recommended",
                epoch, state.used_gb, state.total_gb, state.used_percent
            )
        elif should_reduce_batch:
            self.logger.info(
                "High memory pressure detected (%.2fGB/%.2fGB = %.1f%%), batch reduction recommended",
                state.used_gb, state.total_gb, state.used_percent
            )

        return MemoryDecision(
            state=state,
            should_reduce_batch=should_reduce_batch,
            should_pause=should_pause,
            memory_status=memory_status,
            requires_action=should_reduce_batch or should_pause,
        )

    def _determine_memory_status(self, state: MemoryState) -> str:
        """Determine the current memory status level."""
        if state.used_gb >= self.emergency_limit_gb:
            return "EMERGENCY"
        elif state.used_gb >= self.critical_limit_gb:
            return "CRITICAL"
        elif state.used_gb >= self.warning_limit_gb:
            return "WARNING"
        else:
            return "NORMAL"

    def get_status_info(self, last_decision: MemoryDecision | None) -> dict[str, Any]:
        """
        Get memory status information for system snapshots.
        
        Args:
            last_decision: Last memory decision made, if any
            
        Returns:
            Dictionary containing current memory state and status
        """
        if last_decision is None:
            return {
                "memory_limit_gb": self.config.memory_limit_gb,
                "memory_available_gb": None,
                "memory_status": "NO_CHECK",
                "memory_pressure": False,
            }

        return {
            "memory_limit_gb": last_decision.state.total_gb,
            "memory_available_gb": last_decision.state.available_gb,
            "memory_status": last_decision.memory_status,
            "memory_pressure": last_decision.requires_action,
        }