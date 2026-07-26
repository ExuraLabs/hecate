from collections.abc import Iterator, Sequence
from itertools import cycle

from config.settings import ogmios_settings


class NetworkManager:
    """Manages endpoint rotation for Ogmios connections."""

    _endpoint_cycle: Iterator[str]

    def __init__(self, endpoints: Sequence[str] | None = None):
        endpoints = endpoints or ogmios_settings.endpoints
        self._endpoint_cycle = cycle(endpoints)

    def get_connection(self) -> str:
        """Get the next endpoint in round-robin fashion."""
        return next(self._endpoint_cycle)
