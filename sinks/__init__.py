import logging

from .base import BlockRelay, BufferedSink, DataSink, EpochCoordinator, prepare_block
from .cli import CLISink

logger = logging.getLogger("hecate.sinks")

__all__ = [
    "BlockRelay",
    "BufferedSink",
    "CLISink",
    "DataSink",
    "EpochCoordinator",
    "prepare_block",
]
# Conditionally import the Redis sinks
try:
    from .redis import HistoricalRedisSink, RedisSink

    __all__ += ["HistoricalRedisSink", "RedisSink"]
except ImportError:
    logger.info(
        "Redis support is not available. "
        "Install with 'uv sync --group redis' to enable the Redis sinks."
    )
