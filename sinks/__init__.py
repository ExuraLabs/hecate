import logging

from .base import (
    BlockRelay,
    BufferedSink,
    DataSink,
    EpochCoordinator,
    RollbackRelay,
    prepare_block,
)
from .cli import CLISink

logger = logging.getLogger("hecate.sinks")

__all__ = [
    "BlockRelay",
    "BufferedSink",
    "CLISink",
    "DataSink",
    "EpochCoordinator",
    "RollbackRelay",
    "prepare_block",
]
# Conditionally import the Redis sinks
try:
    from .redis import HistoricalRedisSink, RedisSink
    from .redis_live import RedisLiveSink

    __all__ += ["HistoricalRedisSink", "RedisLiveSink", "RedisSink"]
except ImportError:
    logger.info(
        "Redis support is not available. "
        "Install with 'uv sync --group redis' to enable the Redis sinks."
    )
