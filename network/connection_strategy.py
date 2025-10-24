import logging
from dataclasses import dataclass
from typing import Any

from websockets import ClientConnection, connect

from config.settings import get_ogmios_settings
from network.connection_manager import get_connection_manager

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Configuration for a connection strategy."""
    host: str | None
    port: str | None
    use_connection_manager: bool


class ConnectionStrategy:
    """
    Determines and manages connection strategies for Ogmios clients.
    
    Handles the logic of deciding between direct connections and ConnectionManager
    based on provided parameters, environment variables, and connection preferences.
    """
    
    @staticmethod
    def determine_strategy(
        host: str | None = None,
        port: str | None = None,
        use_managed_connection: bool = False
    ) -> ConnectionConfig:
        """
        Determine the appropriate connection strategy.
        
        Priority order:
        1. Explicit parameters (host/port passed directly)
        2. Environment variables (OGMIOS_HOST/OGMIOS_PORT)
        3. Direct connection fallback (if use_managed_connection=False) 
        4. ConnectionManager (if use_managed_connection=True)
        
        Args:
            host: Explicit host override
            port: Explicit port override
            use_managed_connection: Whether to prefer managed connections over direct ones
            
        Returns:
            ConnectionConfig: Configuration for the determined strategy
            
        Raises:
            ValueError: If parameters are invalid or no valid connection configuration is found
        """
        # Validation 1: Conflicting parameters
        if (host or port) and use_managed_connection:
            raise ValueError(
                "Conflicting parameters: use_managed_connection=True cannot be used "
                "with explicit host/port parameters."
            )

        # Validation 2: Partial parameters - only for explicit parameters
        if (host or port) and not (host and port):
            raise ValueError(
                "Both host and port must be provided together for direct connection. "
            )

        # STRATEGY DETERMINATION
        settings = get_ogmios_settings()

        # Case 1: Explicit parameters provided
        if host and port:
            return ConnectionConfig(
                host=host,
                port=port,
                use_connection_manager=False
            )

        # Case 2: Environment variables configured
        if settings.host and settings.port:
            return ConnectionConfig(
                host=settings.host,
                port=settings.port,
                use_connection_manager=False
            )

        # Case 3: Direct connection fallback (for demo.py and development)
        if not use_managed_connection:
            demo_host, demo_port = "localhost", "1337"
            return ConnectionConfig(
                host=demo_host,
                port=demo_port,
                use_connection_manager=False
            )
        
        # Case 4: Use ConnectionManager
        if settings.endpoints:
            return ConnectionConfig(
                host=None,
                port=None,
                use_connection_manager=True
            )
        
        # Case 5: No valid configuration found
        raise ValueError(
            "No valid Ogmios connection configuration found. "
            "Please configure OGMIOS_HOST/OGMIOS_PORT environment variables "
            "or OGMIOS_ENDPOINTS for ConnectionManager."
        )
    
    @staticmethod
    async def create_connection(
        config: ConnectionConfig,
        path: str = "",
        secure: bool = False,
        **connection_params: Any
    ) -> ClientConnection:
        """
        Create a connection based on the provided configuration.
        
        Args:
            config: Connection configuration from determine_strategy()
            path: WebSocket path
            secure: Whether to use secure connection (wss vs ws)
            **connection_params: Additional connection parameters
            
        Returns:
            ClientConnection: Established WebSocket connection
            
        Raises:
            ConnectionError: If connection cannot be established
        """
        if config.use_connection_manager:
            # Use ConnectionManager
            manager = get_connection_manager()
            connection = await manager.get_connection()
            logger.debug("Connected via ConnectionManager")
            return connection
        else:
            # Direct connection
            protocol = "wss" if secure else "ws"
            connect_str = f"{protocol}://{config.host}:{config.port}{path}"
            try:
                connection = await connect(connect_str, **connection_params)
                logger.debug("Connected directly to %s", connect_str)
                return connection
            except (OSError, ConnectionRefusedError, TimeoutError, ConnectionError) as e:
                raise ConnectionError(
                    f"Failed to connect to {connect_str}. "
                    "Please verify the connection configuration."
                ) from e
