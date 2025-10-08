"""
Process control operations based on memory decisions.

This module contains the ProcessController class responsible for controlling
process execution based on memory pressure decisions.
"""

import asyncio
import logging
from .memory import MemoryDecision


def get_contextual_logger() -> logging.Logger:
    """Get the logger for the current context."""
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except (ImportError, RuntimeError):
        return logging.getLogger(__name__)


class ProcessController:
    """
    Responsible ONLY for process control actions.
    
    Single Responsibility: Pausing and controlling process execution.
    """

    def __init__(self, pause_interval_seconds: int):
        self.pause_interval_seconds = pause_interval_seconds
        self.logger = get_contextual_logger()

    async def pause_if_needed(self, decision: MemoryDecision, epoch: int | None = None) -> None:
        """
        Pause processing if memory decision requires it.
        
        Args:
            decision: Memory decision from policy engine
            epoch: Optional epoch number for logging context
        """
        if not decision.should_pause:
            return

        state = decision.state
        epoch_str = str(epoch) if epoch is not None else '-'
        
        self.logger.warning(
            "Memory emergency threshold reached during epoch %s (%.2fGB/%.2fGB = %.1f%%), pausing processing for memory recovery",
            epoch_str, state.used_gb, state.total_gb, state.used_percent
        )

        await asyncio.sleep(self.pause_interval_seconds)

    async def apply_backpressure(self, duration_seconds: int, reason: str = "") -> None:
        """
        Apply backpressure by pausing for a specific duration.
        
        Args:
            duration_seconds: How long to pause
            reason: Optional reason for the backpressure
        """
        if reason:
            self.logger.info("Applying backpressure for %d seconds: %s", duration_seconds, reason)
        else:
            self.logger.info("Applying backpressure for %d seconds", duration_seconds)
            
        await asyncio.sleep(duration_seconds)