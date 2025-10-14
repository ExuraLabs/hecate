"""
Simple and efficient connection manager for Ogmios WebSocket connections.

This module provides a clean, singleton-based approach to managing WebSocket
connections with automatic fallback and minimal overhead.
"""

import asyncio
import logging
from typing import Any, ClassVar

from websockets import ClientConnection, connect
from websockets.protocol import State

from config.settings import get_ogmios_settings

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Simple, efficient connection manager for Ogmios WebSocket connections.
    
    Uses singleton pattern to ensure single instance across all tasks/workers.
    Loop-aware design ensures connections work correctly across different
    asyncio event loops in multi-worker environments like Dask/Prefect.
    
    Priority logic:
    1. Direct connection via OGMIOS_HOST/OGMIOS_PORT (if both provided)
    2. Round-robin selection from configured endpoints list (fallback)
    
    No complex selection policies, no monitoring overhead, just reliable connections
    with simple load distribution across available endpoints.
    """
    
    _instance: ClassVar["ConnectionManager | None"] = None
    _lock: ClassVar[asyncio.Lock | None] = None
    
    def __new__(cls) -> "ConnectionManager":
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize connection manager (only once due to singleton)."""
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        # Store connections per event loop to avoid cross-loop issues
        self._connections: dict[asyncio.AbstractEventLoop, ClientConnection | None] = {}
        self._connection_url: str | None = None
        self._endpoint_index: int = 0  # Simple counter for round-robin
        self._available_endpoints: list[str] = []
        
        # Determine connection strategy at initialization
        self._setup_connection_strategy()
        logger.info("🔗 Connection manager initialized for: %s", self._connection_url)
    
    @classmethod
    async def get_lock(cls) -> asyncio.Lock:
        """Get or create the async lock for thread-safe operations."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock
    
    def _setup_connection_strategy(self) -> None:
        """
        Setup connection strategy based on configuration.
        
        Determines if we use direct connection or round-robin from endpoints list.
        """
        settings = get_ogmios_settings()
        
        # Priority 1: Direct host/port (simple mode)
        if settings.ogmios_host and settings.ogmios_port:
            self._connection_url = f"ws://{settings.ogmios_host}:{settings.ogmios_port}"
            self._available_endpoints = []  # Direct mode, no rotation
            logger.info("🎯 Using direct connection: %s", self._connection_url)
            return
        
        # Priority 2: Round-robin from endpoints list (fallback mode)
        if settings.endpoints:
            self._available_endpoints = settings.endpoints  # Ya es lista de strings
            self._connection_url = self._available_endpoints[0]  # Start with first
            logger.info("📋 Using round-robin across %d endpoints: %s", 
                       len(self._available_endpoints), self._available_endpoints)
            return
        
        # Fallback: localhost default
        self._connection_url = "ws://localhost:1337"
        self._available_endpoints = []
        logger.warning("⚠️  No configuration found, using default: %s", self._connection_url)
    
    def _get_next_endpoint(self) -> str:
        """
        Get next endpoint URL using simple round-robin.
        
        Returns:
            str: Next WebSocket URL to try
        """
        # If no rotation (direct mode or single endpoint), return current URL
        if len(self._available_endpoints) <= 1:
            return self._connection_url or "ws://localhost:1337"
        
        # Round-robin through available endpoints
        self._endpoint_index = (self._endpoint_index + 1) % len(self._available_endpoints)
        next_url = self._available_endpoints[self._endpoint_index]
        
        logger.debug("🔄 Round-robin selecting endpoint %d/%d: %s", 
                    self._endpoint_index + 1, len(self._available_endpoints), next_url)
        return next_url
    
    async def get_connection(self) -> ClientConnection:
        """
        Get active WebSocket connection for current event loop.
        
        Thread-safe operation with automatic reconnection on failure.
        Each asyncio event loop gets its own connection to avoid cross-loop issues.
        
        Returns:
            ClientConnection: Active WebSocket connection for current loop
            
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
            ConnectionError: If connection fails after retries
        """
        if not self._connection_url:
            raise ConnectionError("No connection URL configured")
        
        max_retries = 3
        last_error = None
        
        # Try current endpoint first, then round-robin if available
        endpoints_to_try = [self._connection_url]
        if len(self._available_endpoints) > 1:
            # Add other endpoints for failover (excluding current)
            other_endpoints = [ep for ep in self._available_endpoints if ep != self._connection_url]
            endpoints_to_try.extend(other_endpoints)
        
        for endpoint_url in endpoints_to_try:
            for attempt in range(max_retries):
                try:
                    logger.debug("Connecting to %s (attempt %d/%d)", 
                               endpoint_url, attempt + 1, max_retries)
                    
                    connection = await connect(endpoint_url)
                    
                    # Update current URL if we succeeded with a different endpoint
                    if endpoint_url != self._connection_url:
                        self._connection_url = endpoint_url
                        # Update index to match the successful endpoint
                        if endpoint_url in self._available_endpoints:
                            self._endpoint_index = self._available_endpoints.index(endpoint_url)
                        logger.info("✅ Switched to endpoint: %s", endpoint_url)
                    else:
                        logger.info("✅ Connected to %s", endpoint_url)
                    
                    return connection
                    
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        logger.debug("Connection attempt %d failed for %s: %s, retrying...", 
                                   attempt + 1, endpoint_url, e)
                        await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        logger.warning("All %d attempts failed for %s: %s", 
                                     max_retries, endpoint_url, e)
        
        # If we get here, all endpoints failed
        raise ConnectionError(f"Cannot connect to any endpoint. Last error: {last_error}") from last_error
    
    async def close(self) -> None:
        """Close all active connections across all event loops."""
        lock = await self.get_lock()
        async with lock:
            for loop, connection in list(self._connections.items()):
                if connection:
                    try:
                        await connection.close()
                        logger.info("🔌 Connection closed for loop %s", id(loop))
                    except Exception as e:
                        logger.warning("Error closing connection for loop %s: %s", id(loop), e)
            
            # Clear all connections
            self._connections.clear()
    
    async def close_current_loop(self) -> None:
        """Close connection for current event loop only."""
        current_loop = asyncio.get_running_loop()
        lock = await self.get_lock()
        async with lock:
            connection = self._connections.get(current_loop)
            if connection:
                try:
                    await connection.close()
                    logger.info("🔌 Connection closed for current loop")
                except Exception as e:
                    logger.warning("Error closing connection for current loop: %s", e)
                finally:
                    del self._connections[current_loop]
    
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