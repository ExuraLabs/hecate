import asyncio
import logging
import os
from typing import Any

from websockets import ClientConnection, connect
from websockets.protocol import State

from config.settings import get_ogmios_settings, get_concurrency_settings

logger = logging.getLogger(__name__)


class ConnectionManager:
    @staticmethod
    async def cleanup_pool():
        """
        Clean up all connections in the current process's pool.
        This should be called at the end of each batch to avoid event loop issues.
        """
        pid = os.getpid()
        instance = ConnectionManager._instances.get(pid)
        if instance is not None:
            await instance.close()
    """
    High-performance connection manager with true connection pooling.
    
    Provides efficient connection reuse for Prefect tasks while maintaining
    simplicity and reliability.
    """

    # Multiprocessing-safe singleton: one instance per process (by PID)
    _instances: dict[int, "ConnectionManager"] = {}
    _lock = asyncio.Lock()

    def __new__(cls) -> "ConnectionManager":
        """Ensure singleton instance per process (by PID)."""
        pid = os.getpid()
        if pid not in cls._instances:
            cls._instances[pid] = super().__new__(cls)
        return cls._instances[pid]
    
    def __init__(self) -> None:
        """Initialize connection manager with efficient pooling strategy."""
        if hasattr(self, '_initialized'):
            return

        self._initialized = True

        # Simplified connection pool - single pool for all tasks
        self._available_connections: list[ClientConnection] = []
        self._in_use_connections: set[ClientConnection] = set()

        # Configuration from settings
        concurrency_settings = get_concurrency_settings()
        self._max_pool_size = concurrency_settings.connection_pool_size
        self._initial_connections = concurrency_settings.initial_connections

        # Endpoint configuration
        self._endpoints: list[str] = []
        self._current_endpoint_index = 0

        self._setup_endpoints()
        logger.info("ConnectionManager initialized with max pool size: %d", self._max_pool_size)

    def _setup_endpoints(self) -> None:
        """Setup endpoint configuration for connection creation."""
        settings = get_ogmios_settings()

        if settings.endpoints:
            self._endpoints = settings.endpoints
            logger.info("Configured %d endpoints for connection pooling", len(self._endpoints))
        else:
            raise ConnectionError("No endpoints configured in Ogmios settings")

    async def get_connection(self) -> ClientConnection:
        """
        Get a WebSocket connection from the pool.
        
        Returns an available connection from the pool, or creates a new one
        if needed and under the pool size limit.
        
        Returns:
            ClientConnection: WebSocket connection ready for use
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        async with self._lock:
            # Try to get an available connection from pool
            for connection in self._available_connections[:]:
                if (hasattr(connection, 'state') and 
                    connection.state == State.OPEN and
                    connection not in self._in_use_connections):

                    self._available_connections.remove(connection)
                    self._in_use_connections.add(connection)
                    logger.debug("Reused connection from pool (available: %d, in use: %d)", 
                               len(self._available_connections), len(self._in_use_connections))
                    return connection

            # No available connections - create new if under limit
            total_connections = len(self._available_connections) + len(self._in_use_connections)
            if total_connections < self._max_pool_size:
                new_connection = await self._create_connection()
                self._in_use_connections.add(new_connection)
                logger.debug("Created new connection (total: %d, in use: %d)", 
                           total_connections + 1, len(self._in_use_connections))
                return new_connection

            # Pool exhausted - wait for connection to become available
            logger.warning("Connection pool exhausted (%d connections), waiting...", total_connections)

        # Brief wait before retrying (outside lock to allow releases)
        await asyncio.sleep(0.1)
        return await self.get_connection()  # Recursive retry with small delay

    async def _create_connection(self) -> ClientConnection:
        """
        Create a new WebSocket connection using round-robin endpoint selection.
        
        Returns:
            ClientConnection: New WebSocket connection
            
        Raises:
            ConnectionError: If all endpoints fail
        """
        if not self._endpoints:
            raise ConnectionError("No endpoints configured")

        # Round-robin endpoint selection
        endpoint_url = self._endpoints[self._current_endpoint_index]
        
        try:
            logger.debug("Creating connection to: %s", endpoint_url)
            connection = await connect(endpoint_url)
            logger.info("Connected to Ogmios: %s", endpoint_url)
            return connection
        except Exception as e:
            logger.warning("Endpoint failed: %s (%s)", endpoint_url, e)
            # Try other endpoints if available
            if len(self._endpoints) > 1:
                return await self._try_fallback_endpoints()
            else:
                raise ConnectionError(f"Cannot connect to {endpoint_url}: {e}") from e
    
    async def _try_fallback_endpoints(self) -> ClientConnection:
        """
        Try connecting to fallback endpoints using round-robin.
        
        Returns:
            ClientConnection: New WebSocket connection
            
        Raises:
            ConnectionError: If all endpoints fail
        """
        original_index = self._current_endpoint_index

        # Try all other endpoints
        for _ in range(len(self._endpoints) - 1):
            self._current_endpoint_index = (self._current_endpoint_index + 1) % len(self._endpoints)
            endpoint_url = self._endpoints[self._current_endpoint_index]

            try:
                logger.debug("Trying fallback endpoint: %s", endpoint_url)
                connection = await connect(endpoint_url)
                logger.info("Connected to Ogmios (fallback): %s", endpoint_url)
                return connection
                
            except Exception as e:
                logger.warning("Fallback endpoint failed: %s (%s)", endpoint_url, e)
                continue

        # All endpoints failed - restore original index and raise
        self._current_endpoint_index = original_index
        raise ConnectionError(f"Cannot connect to any of {len(self._endpoints)} endpoints")
    
    async def release_connection(self, connection: ClientConnection) -> None:
        """
        Release connection back to the pool for reuse.
        
        This is essential for efficient connection pooling.
        
        Args:
            connection: The connection to release back to pool
        """
        async with self._lock:
            # Remove from in-use set
            self._in_use_connections.discard(connection)

            # Check if connection is still healthy
            if (hasattr(connection, 'state') and 
                connection.state == State.OPEN):
                # Return healthy connection to available pool
                self._available_connections.append(connection)
                logger.debug("Released healthy connection to pool (available: %d, in use: %d)", 
                           len(self._available_connections), len(self._in_use_connections))
            else:
                # Close and discard unhealthy connection
                try:
                    await connection.close()
                except Exception:
                    pass  # Ignore errors when closing dead connection
                logger.debug("Discarded unhealthy connection (available: %d, in use: %d)", 
                           len(self._available_connections), len(self._in_use_connections))

    async def close(self) -> None:
        """Close all connections and clean up the pool."""
        async with self._lock:
            # Close all connections
            all_connections = list(self._available_connections) + list(self._in_use_connections)
            for connection in all_connections:
                try:
                    await connection.close()
                    logger.debug("Connection closed")
                except Exception as e:
                    logger.warning("Error closing connection: %s", e)
            
            # Clear all collections
            self._available_connections.clear()
            self._in_use_connections.clear()
            logger.info("Connection pool closed and cleaned up")

    async def __aenter__(self) -> "ConnectionManager":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# Convenience function for getting the singleton instance
def get_connection_manager() -> ConnectionManager:
    """
    Get the singleton ConnectionManager instance.
    
    Returns:
        ConnectionManager: Singleton connection manager instance
    """
    return ConnectionManager()