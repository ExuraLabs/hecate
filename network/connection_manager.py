import asyncio
import logging
from typing import Any, ClassVar

from websockets import ClientConnection, connect
from websockets.protocol import State

from config.settings import get_ogmios_settings

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Simple, efficient connection manager for Ogmios WebSocket connections.22
    """
    
    _instance: ClassVar["ConnectionManager | None"] = None
    _locks: ClassVar[dict[asyncio.AbstractEventLoop, asyncio.Lock]] = {}  # Lock per event loop
    _global_endpoint_counter: ClassVar[int] = 0  # Round-robin counter across all instances
    
    def __new__(cls) -> "ConnectionManager":
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """
        Initialize connection manager
        
        Currently stores one connection per event loop for simplicity.
        Future enhancement: Connection pooling and multiplexing across endpoints.
        """
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        # Store connections per event loop to avoid cross-loop issues
        # TODO: Future - expand to connection pool per loop for multiplexing
        self._connections: dict[asyncio.AbstractEventLoop, ClientConnection | None] = {}
        self._connection_url: str | None = None
        self._available_endpoints: list[str] = []
        self._current_endpoint_index: int = 0
        
        # Determine connection strategy at initialization
        self._setup_connection_strategy()
        logger.info("Connection manager initialized for: %s", self._connection_url)
    
    @classmethod
    async def get_lock(cls) -> asyncio.Lock:
        """Get or create the async lock for the current event loop."""
        current_loop = asyncio.get_running_loop()
        
        # Check if we already have a lock for this event loop
        if current_loop not in cls._locks:
            cls._locks[current_loop] = asyncio.Lock()
        
        return cls._locks[current_loop]
    
    def _setup_connection_strategy(self) -> None:
        """
        Setup connection strategy using only endpoints list with round-robin.
        
        Future enhancement: This will support multiple connections per endpoint
        for true multiplexing when that feature is discussed and implemented.
        """
        settings = get_ogmios_settings()
        
        # Only use endpoints list - remove direct host/port logic
        if settings.endpoints:
            self._available_endpoints = settings.endpoints
            # Simple round-robin without lock for now
            # TODO: Make this async lock-based when connection pooling is added
            ConnectionManager._global_endpoint_counter = (
                ConnectionManager._global_endpoint_counter + 1
            ) % len(self._available_endpoints)
            
            self._current_endpoint_index = ConnectionManager._global_endpoint_counter
            self._connection_url = self._available_endpoints[self._current_endpoint_index]
            logger.info("Using round-robin endpoint %s", self._connection_url)
        else:
            raise ConnectionError("No endpoints configured in settings")
    
    async def get_connection(self) -> ClientConnection:
        """
        Get active WebSocket connection for current event loop.
        
        Thread-safe operation with automatic reconnection on failure.
        
        Returns:
            ClientConnection: Active WebSocket connection
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        # Get current event loop to isolate connections per loop
        current_loop = asyncio.get_running_loop()
        
        lock = await self.get_lock()
        async with lock:
            # Check if existing connection for this loop is still valid
            existing_connection = self._connections.get(current_loop)
            if (existing_connection and 
                hasattr(existing_connection, 'state') and 
                existing_connection.state == State.OPEN):
                return existing_connection
            
            # Create new connection for this loop
            new_connection = await self._create_connection()
            self._connections[current_loop] = new_connection
            return new_connection
    
    async def _create_connection(self) -> ClientConnection:
        """
        Create new WebSocket connection with retry logic and optional round-robin.
        
        Returns:
            ClientConnection: New WebSocket connection
            
        Raises:
            ConnectionError: If all endpoints fail
        """
        if not self._available_endpoints:
            raise ConnectionError("No endpoints configured")
        
        # Try current endpoint first (sticky behavior)
        try:
            logger.debug("Connecting to current endpoint: %s", self._connection_url)
            connection = await connect(self._connection_url)
            logger.info("Connected to Ogmios (sticky): %s", self._connection_url)
            return connection
            
        except Exception as e:
            logger.warning("Current endpoint failed: %s (%s)", self._connection_url, e)
            
            # If we have multiple endpoints, try fallback
            if len(self._available_endpoints) > 1:
                return await self._try_fallback_endpoints()
            else:
                # Only one endpoint, re-raise the error
                raise ConnectionError(f"Cannot connect to {self._connection_url}: {e}") from e
    
    async def _try_fallback_endpoints(self) -> ClientConnection:
        """
        Try connecting to fallback endpoints using round-robin rotation.
        
        Returns:
            ClientConnection: New WebSocket connection
            
        Raises:
            ConnectionError: If all endpoints fail
        """
        original_index = self._current_endpoint_index
        
        # Try all other endpoints once
        for _ in range(len(self._available_endpoints) - 1):
            # Rotate to next endpoint
            self._current_endpoint_index = (self._current_endpoint_index + 1) % len(self._available_endpoints)
            self._connection_url = self._available_endpoints[self._current_endpoint_index]
            
            try:
                logger.debug("Trying fallback endpoint: %s", self._connection_url)
                connection = await connect(self._connection_url)
                logger.info("Connected to Ogmios (fallback): %s", self._connection_url)
                return connection
                
            except Exception as e:
                logger.warning("Fallback endpoint failed: %s (%s)", self._connection_url, e)
                continue
        
        # All endpoints failed, restore original index and raise error
        self._current_endpoint_index = original_index
        self._connection_url = self._available_endpoints[self._current_endpoint_index]
        raise ConnectionError(f"Cannot connect to any of {len(self._available_endpoints)} endpoints")
    
    async def close(self) -> None:
        """Close all active connections across all event loops."""
        current_loop = asyncio.get_running_loop()
        lock = await self.get_lock()
        async with lock:
            for loop, connection in list(self._connections.items()):
                if connection:
                    try:
                        await connection.close()
                        logger.info("Connection closed for loop %s", id(loop))
                    except Exception as e:
                        logger.warning("Error closing connection for loop %s: %s", id(loop), e)
            
            # Clear all connections
            self._connections.clear()
            
            # Clean up lock for current loop
            if current_loop in self._locks:
                del self._locks[current_loop]
    
    async def close_current_loop(self) -> None:
        """Close connection for current event loop only."""
        current_loop = asyncio.get_running_loop()
        lock = await self.get_lock()
        async with lock:
            connection = self._connections.get(current_loop)
            if connection:
                try:
                    await connection.close()
                    logger.info("Connection closed for current loop")
                except Exception as e:
                    logger.warning("Error closing connection for current loop: %s", e)
                finally:
                    del self._connections[current_loop]
            
            # Clean up lock for current loop
            if current_loop in self._locks:
                del self._locks[current_loop]
    
    def mark_connection_for_reuse(self, connection: ClientConnection) -> None:
        """
        Mark a connection as available for reuse (placeholder for future pooling).
        
        Currently this does nothing, but will be used when connection pooling
        and multiplexing across endpoints is implemented.
        
        Args:
            connection: The connection to mark for reuse
        """
        # TODO: Implement connection pooling logic here
        logger.debug("Connection marked for potential reuse (feature pending)")
    
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