"""
Memory management package.

This package provides memory management functionality following the Single
Responsibility Principle (SRP) while maintaining backward compatibility.

Main exports:
- AdaptiveMemoryController: Main facade for memory management
- MemoryConfig: Configuration for memory management
- MemoryState: Represents memory state data
- MemoryDecision: Represents memory policy decisions
"""

from .controller import AdaptiveMemoryController
from .memory import MemoryConfig, MemoryState, MemoryDecision

__all__ = [
    "AdaptiveMemoryController",
    "MemoryConfig", 
    "MemoryState",
    "MemoryDecision",
]