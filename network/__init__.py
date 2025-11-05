from itertools import cycle
from typing import Iterator

from config.settings import ogmios_settings


class NetworkManager:
    """Manages endpoint rotation for Ogmios connections."""

    _endpoint_cycle: Iterator[str]

    def __init__(self, endpoints: list[str] | None = None):
        endpoints = endpoints or ogmios_settings.endpoints
        self._endpoint_cycle = cycle(endpoints)

    def get_connection(self) -> str:
        """Get the next endpoint in round-robin fashion."""
        return next(self._endpoint_cycle)
