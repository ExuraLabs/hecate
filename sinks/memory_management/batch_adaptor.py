import logging
from .memory import MemoryDecision


def get_contextual_logger() -> logging.Logger:
    """Get the logger for the current context."""
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except (ImportError, RuntimeError):
        return logging.getLogger(__name__)


class BatchSizeAdaptor:
    """
    Responsible ONLY for calculating optimal batch sizes
    """

    def __init__(self, min_batch_size: int, max_batch_size: int):
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.logger = get_contextual_logger()

    def calculate_optimal_batch_size(
        self, 
        decision: MemoryDecision, 
        current_batch_size: int, 
        original_batch_size: int
    ) -> int:
        """
        Calculate the optimal batch size based on memory decision.
        
        Args:
            decision: Memory decision from policy engine
            current_batch_size: Current batch size being used
            original_batch_size: Original/maximum batch size
            
        Returns:
            Optimal batch size to use
        """
        if decision.should_reduce_batch:
            return self._reduce_batch_size(current_batch_size)
        elif current_batch_size < original_batch_size and not decision.requires_action:
            return self._increase_batch_size(current_batch_size, original_batch_size)
        else:
            return current_batch_size

    def _reduce_batch_size(self, current_batch_size: int) -> int:
        """Reduce batch size due to memory pressure."""
        optimal_batch_size = max(self.min_batch_size, current_batch_size // 2)
        
        if optimal_batch_size != current_batch_size:
            self.logger.info(
                "Reducing batch size due to memory pressure: %d -> %d",
                current_batch_size, optimal_batch_size
            )
        
        return optimal_batch_size

    def _increase_batch_size(self, current_batch_size: int, original_batch_size: int) -> int:
        """Increase batch size when memory pressure is normal."""
        optimal_batch_size = min(self.max_batch_size, current_batch_size + 100)
        optimal_batch_size = min(optimal_batch_size, original_batch_size)
        
        if optimal_batch_size != current_batch_size:
            self.logger.debug(
                "Increasing batch size (memory pressure normal): %d -> %d",
                current_batch_size, optimal_batch_size
            )
        
        return optimal_batch_size