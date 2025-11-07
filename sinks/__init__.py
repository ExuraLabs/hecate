import logging

from .cli import CLISink
from .base import BufferedSink


logger = logging.getLogger("hecate.sinks")

__all__ = ["BufferedSink", "CLISink"]
# Conditionally import RedisSink
try:
    from .redis import RedisSink

    __all__ += ["RedisSink"]
except ImportError:
    logger.info(
        "Redis support is not available."
        "Install with 'uv sync --group redis' to enable the RedisSink."
    )
