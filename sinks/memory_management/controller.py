import logging
import time
import threading
from typing import Any

from config.settings import get_batch_settings, get_memory_settings
from .memory import MemoryConfig, MemoryMonitor, MemoryPolicyEngine, MemoryDecision, MemoryState
from .batch_adaptor import BatchSizeAdaptor
from .process_control import ProcessController


def get_contextual_logger() -> logging.Logger:
    """Get the logger for the current context."""
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except (ImportError, RuntimeError):
        return logging.getLogger(__name__)


class AdaptiveMemoryController:
    """
    Main controller that orchestrates memory management components.
    
    This class acts as a facade, maintaining the original API while internally
    using the new SRP-compliant architecture with specialized components.
    
    Responsibilities:
    - Orchestrate the specialized components
    - Maintain backward compatibility
    - Provide the main public interface
    - Ensure single instance per flow execution
    """
    
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls, config: MemoryConfig | None = None) -> "AdaptiveMemoryController":
        """
        Thread-safe singleton implementation for Dask workers.
        
        In Prefect 3.3.3 with Dask, each worker process may try to create
        an instance simultaneously. This ensures only one instance exists
        per worker process.
        """
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: MemoryConfig | None = None):
        # Only initialize once, even if __init__ is called multiple times
        if self.__class__._initialized:
            return
            
        with self.__class__._lock:
            if self.__class__._initialized:
                return
                
            if config is None:
                memory_settings = get_memory_settings()
                config = MemoryConfig(**memory_settings.model_dump())

            self.config = config
            self._last_check_time: float = 0
            self._last_decision: MemoryDecision | None = None
            
            # Initialize specialized components
            self.monitor = MemoryMonitor(config)
            self.policy_engine = MemoryPolicyEngine(config)
            
            batch_settings = get_batch_settings()
            self.batch_adaptor = BatchSizeAdaptor(
                min_batch_size=batch_settings.min_size,
                max_batch_size=batch_settings.max_size
            )
            
            self.process_controller = ProcessController(
                pause_interval_seconds=config.check_interval_seconds
            )

            self.logger = get_contextual_logger()
            
            # Mark as initialized FIRST (class variable, not instance)
            self.__class__._initialized = True
            
            # Log initialization message only once (when actually initializing)
            self.logger.info(
                "AdaptiveMemoryController initialized with limit: %.2f GB",
                self.config.memory_limit_gb
            )
            self.logger.debug(
                "  - Warning threshold:  %.2f GB (%.0f%%)",
                config.memory_limit_gb * config.warning_threshold, 
                config.warning_threshold * 100
            )
            self.logger.debug(
                "  - Critical threshold: %.2f GB (%.0f%%)",
                config.memory_limit_gb * config.critical_threshold, 
                config.critical_threshold * 100
            )
            self.logger.debug(
                "  - Emergency threshold: %.2f GB (%.0f%%)",
                config.memory_limit_gb * config.emergency_threshold, 
                config.emergency_threshold * 100
            )

    @classmethod
    def reset_singleton(cls) -> None:
        """
        Reset the singleton instance.
        
        Useful for testing or when starting a new flow execution.
        Should be called with caution in production.
        """
        with cls._lock:
            cls._instance = None
            cls._initialized = False

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if the singleton instance is already initialized."""
        return cls._initialized

    def check_memory_and_adapt(
        self, epoch: int, original_batch_size: int, current_batch_size: int
    ) -> MemoryDecision | None:
        """
        Check memory state and provide adaptation decisions.
        
        Args:
            epoch: Current epoch being processed (for logging)
            original_batch_size: The original/maximum batch size
            current_batch_size: The current batch size being used
            
        Returns:
            MemoryDecision with all decision data, or None if no check was performed
        """
        now = time.time()
        time_since_last_check = now - self._last_check_time

        should_check_memory = (
            self._last_decision is None
            or time_since_last_check >= self.config.check_interval_seconds
        )

        if not should_check_memory:
            return None

        self._last_check_time = now
        
        # Use specialized components to make decision
        state = self.monitor.get_current_memory_state(force_refresh=True)
        decision = self.policy_engine.evaluate_memory_state(state, epoch)
        
        self._last_decision = decision
        return decision

    async def handle_memory_management(
        self, epoch: int, original_batch_size: int, current_batch_size: int
    ) -> int:
        """
        High-level method that handles all memory management concerns.
        
        This method orchestrates all components to provide complete memory
        management functionality while maintaining the original API.
        
        Args:
            epoch: Current epoch being processed
            original_batch_size: The original/maximum batch size
            current_batch_size: Current batch size being used
            
        Returns:
            The optimal batch size to use (may be unchanged)
        """
        decision = self.check_memory_and_adapt(epoch, original_batch_size, current_batch_size)
        
        if decision is None:
            return current_batch_size

        # Use specialized components for actions
        await self.process_controller.pause_if_needed(decision, epoch)
        
        optimal_batch_size = self.batch_adaptor.calculate_optimal_batch_size(
            decision, current_batch_size, original_batch_size
        )

        self.logger.debug(
            "Memory check performed: %s, batch size: %d",
            decision.memory_status, optimal_batch_size
        )

        return optimal_batch_size

    async def pause_processing_if_needed(self, epoch: int | None = None) -> None:
        """
        Pause processing when memory pressure requires it.
        Uses the last memory check result.
        
        Args:
            epoch: Optional epoch number for logging context
        """
        if self._last_decision is None:
            return
            
        await self.process_controller.pause_if_needed(self._last_decision, epoch)

    # Backward compatibility methods
    def should_reduce_batch_size(self) -> bool:
        """Returns True if memory usage is above the critical threshold."""
        return getattr(self._last_decision, "should_reduce_batch", False)

    def should_pause_processing(self) -> bool:
        """Returns True if memory usage is above the emergency threshold."""
        if self._last_decision is None:
            return False
        return self._last_decision.should_pause

    def get_optimal_batch_size(
        self, original_batch_size: int, current_batch_size: int
    ) -> int:
        """
        Returns the optimal batch size based on current memory pressure.
        
        Args:
            original_batch_size: The original/maximum batch size
            current_batch_size: The current batch size being used
            
        Returns:
            The optimal batch size to use
        """
        if self._last_decision is None:
            return current_batch_size

        return self.batch_adaptor.calculate_optimal_batch_size(
            self._last_decision, current_batch_size, original_batch_size
        )

    def get_snapshot_info(self) -> dict[str, Any]:
        """
        Get memory controller information for system snapshots.
        
        Returns:
            Dictionary containing current memory state and configuration
        """
        return self.policy_engine.get_status_info(self._last_decision)

    def format_memory_info(self, base_memory_info: str) -> str:
        """
        Format memory information for display, including controller status.
        
        Args:
            base_memory_info: Base memory information string
            
        Returns:
            Formatted memory information with controller status
        """
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

    def get_memory_state(self, force_refresh: bool = False) -> MemoryState:
        """Get the current memory state (backward compatibility)."""
        return self.monitor.get_current_memory_state(force_refresh)