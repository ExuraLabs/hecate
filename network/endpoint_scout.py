import asyncio
import logging
import time
from typing import Protocol
from dataclasses import dataclass
from websockets import ClientConnection, connect, ConnectionClosed
from pydantic import WebsocketUrl

logger = logging.getLogger(__name__)


@dataclass
class ConnectionMetrics:
    """Connection metrics for an endpoint."""

    latency_ms: float | None = None
    last_ping: float | None = None
    connection_errors: int = 0
    last_error_time: float | None = None


class SelectionPolicy(Protocol):
    """Protocol for endpoint selection policies."""

    def select_endpoint(
        self, candidates: list[tuple[WebsocketUrl, ConnectionMetrics]]
    ) -> WebsocketUrl:
        """Select the best endpoint from a list of candidates."""
        ...


class LatencyBasedPolicy:
    """Selects the endpoint with the lowest latency."""

    def select_endpoint(
        self, candidates: list[tuple[WebsocketUrl, ConnectionMetrics]]
    ) -> WebsocketUrl:
        """Select the endpoint with the lowest latency from valid candidates."""
        valid_candidates = [
            (url, metrics)
            for url, metrics in candidates
            if metrics.latency_ms is not None
        ]

        if not valid_candidates:
            # Fallback to first if no valid metrics
            return candidates[0][0]

        return min(valid_candidates, key=lambda x: x[1].latency_ms or float("inf"))[0]


class WeightedLatencyPolicy:
    """
    Policy that combines configured weight with measured latency.
    Useful for preferring certain endpoints unless latency is too high.
    """

    def __init__(self, latency_threshold_ms: float = 1000):
        self.latency_threshold_ms = latency_threshold_ms

    def select_endpoint(
        self, candidates: list[tuple[WebsocketUrl, ConnectionMetrics]]
    ) -> WebsocketUrl:
        """Select endpoint using weighted random based on latency."""
        import random

        acceptable = [
            (url, metrics)
            for url, metrics in candidates
            if metrics.latency_ms is None
            or metrics.latency_ms < self.latency_threshold_ms
        ]

        if not acceptable:
            # If all have high latency, use best available
            acceptable = candidates

        weights = []
        urls = []

        for url, metrics in acceptable:
            if metrics.latency_ms is None:
                weight = 1.0
            else:
                # Invert latency so lower latency = higher weight
                weight = 1000 / (
                    metrics.latency_ms + 100
                )  # +100 to avoid division by zero

            weights.append(weight)
            urls.append(url)

        # Weighted random selection
        return random.choices(urls, weights=weights)[0]


class EndpointScout:
    """
    Manages WebSocket connections to Ogmios endpoints.
    
    Supports two modes:
    1. Simple mode: Single endpoint from OGMIOS_URL/OGMIOS_PORT (bypasses selection logic)
    2. Multi-endpoint mode: Tests and selects best endpoint from configured list
    """

    def __init__(
        self,
        selection_policy: SelectionPolicy | None = None,
    ):
        """
        Initialize the EndpointScout.
        Args:
            selection_policy: Policy for selecting the best endpoint
        """
        # Determine mode and endpoints
        self.simple_mode, self.endpoints = self._determine_mode_and_endpoints()
        self.selection_policy = selection_policy or LatencyBasedPolicy()
        self.connections: dict[WebsocketUrl, ClientConnection | None] = {}
        self.metrics: dict[WebsocketUrl, ConnectionMetrics] = {}
        self._monitoring_task: asyncio.Task[None] | None = None

    def _determine_mode_and_endpoints(
        self
    ) -> tuple[bool, list[WebsocketUrl]]:
        """Determine connection mode and get appropriate endpoints."""
        from config.settings import get_ogmios_settings
        settings = get_ogmios_settings()

        # Simple mode: if both ogmios_host and ogmios_port are provided (EXCLUSIVE mode)
        if settings.ogmios_host and settings.ogmios_port:
            url = f"ws://{settings.ogmios_host}:{settings.ogmios_port}"
            logger.info(f"🔗 Simple connection mode: {url}")
            return True, [WebsocketUrl(url)]

        # Multi-endpoint mode: always load from settings
        configured_endpoints = [WebsocketUrl(ep["url"]) for ep in settings.endpoints]
        logger.info(f"🔍 Multi-endpoint mode: {len(configured_endpoints)} endpoints from settings")
        return False, configured_endpoints

    @staticmethod
    def _load_endpoints_from_settings() -> list[WebsocketUrl]:
        try:
            from config.settings import get_ogmios_settings

            settings = get_ogmios_settings()
            endpoints = [
                WebsocketUrl(ep["url"]) for ep in settings.endpoints if ep.get("url")
            ]
            logger.info(
                "Loaded %d endpoints from settings: %s",
                len(endpoints),
                [str(ep) for ep in endpoints],
            )
            return endpoints
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Failed to load endpoints from settings: %s", e)
            return []

    async def get_best_connection(self) -> ClientConnection:
        """
        Returns the best available connection based on mode.
        
        In simple mode: connects directly without selection logic
        In multi-endpoint mode: uses existing selection logic
        """
        if self.simple_mode:
            return await self._get_simple_connection()
        else:
            return await self._get_multi_endpoint_connection()

    async def _get_simple_connection(self) -> ClientConnection:
        """Get connection in simple mode - direct connection with monitoring."""
        url = self.endpoints[0]

        # Check existing connection
        existing_conn = self.connections.get(url)
        if existing_conn and hasattr(existing_conn, "open") and existing_conn.open:
            return existing_conn

        # Create new connection
        try:
            logger.debug(f"Establishing simple connection to {url}")
            start_time = time.perf_counter()
            conn = await connect(str(url))
            end_time = time.perf_counter()

            latency = (end_time - start_time) * 1000
            self.connections[url] = conn
            self.metrics[url] = ConnectionMetrics(
                latency_ms=latency,
                last_ping=time.time()
            )

            logger.info(f"✅ Simple connection established: {url} ({latency:.2f}ms)")
            return conn

        except Exception as e:
            logger.error(f"❌ Simple connection failed to {url}: {e}")
            raise ConnectionError(f"Failed to establish simple connection to {url}: {e}")

    async def _get_multi_endpoint_connection(self) -> ClientConnection:
        """Get best connection using existing multi-endpoint logic."""
        # Try to use existing healthy connection
        healthy_connections = [
            (url, conn)
            for url, conn in self.connections.items()
            if conn and hasattr(conn, "open") and conn.open
        ]

        if healthy_connections:
            candidates = [
                (url, self.metrics.get(url, ConnectionMetrics()))
                for url, _ in healthy_connections
            ]
            best_url = self.selection_policy.select_endpoint(candidates)
            best_connection = self.connections[best_url]
            if best_connection:
                return best_connection

        # If no healthy connections, establish new ones
        return await self._establish_best_connection()

    async def _establish_best_connection(self) -> ClientConnection:
        """
        Establishes connection to the best available endpoint.
        Tests all configured endpoints and selects the first successful one (lowest latency).
        Returns:
            ClientConnection: The best available WebSocket connection
        Raises:
            ConnectionError: If no connection can be established
        """
        endpoints_to_try = self.endpoints
        if not endpoints_to_try:
            logger.error("No endpoints configured, attempting to load from settings")
            endpoints_to_try = self._load_endpoints_from_settings()

        if not endpoints_to_try:
            raise ConnectionError(
                "No endpoints available for connection. Check your .env and settings configuration."
            )

        logger.info(
            "Attempting to connect to %d endpoints: %s",
            len(endpoints_to_try),
            [str(ep) for ep in endpoints_to_try],
        )
        connection_attempts = []

        for url in endpoints_to_try:
            success = False
            last_error = None

            # Retry up to 3 times per endpoint
            for attempt in range(3):
                try:
                    logger.debug(
                        "Attempting connection to %s (attempt %d/3)", url, attempt + 1
                    )
                    start_time = time.perf_counter()
                    conn = await connect(str(url))
                    end_time = time.perf_counter()
                    latency = (end_time - start_time) * 1000
                    self.connections[url] = conn
                    self.metrics[url] = ConnectionMetrics(
                        latency_ms=latency, last_ping=time.time()
                    )
                    connection_attempts.append((url, latency, conn))
                    logger.info(
                        "Successfully connected to %s with %.2fms latency (attempt %d)",
                        url,
                        latency,
                        attempt + 1,
                    )
                    success = True
                    break
                except (ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                    last_error = e
                    if attempt < 2:
                        logger.debug(
                            "Connection attempt %d failed for %s: %s, retrying...",
                            attempt + 1,
                            url,
                            e,
                        )
                        await asyncio.sleep(0.5)
                    else:
                        logger.warning(
                            "All 3 connection attempts failed for %s: %s", url, e
                        )

            if not success and last_error:
                self.metrics[url] = ConnectionMetrics(latency_ms=None, last_ping=None)

        if not connection_attempts:
            raise ConnectionError("No endpoints available for connection")

        # Select the best established connection (lowest latency)
        best_url, _, best_conn = min(connection_attempts, key=lambda x: x[1])
        logger.info("Selected best endpoint: %s", best_url)

        # Close other connections to avoid resource waste
        for url, _, conn in connection_attempts:
            if url != best_url:
                await conn.close()
                self.connections[url] = None

        return best_conn

    async def start_monitoring(self) -> None:
        """Start continuous endpoint monitoring."""
        if self._monitoring_task and not self._monitoring_task.done():
            logger.debug("Monitoring task already running")
            return

        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("🔍 Started endpoint monitoring")

    async def stop_monitoring(self) -> None:
        """Stop continuous endpoint monitoring."""
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("Stopped endpoint monitoring")

    async def _monitoring_loop(self) -> None:
        """Monitoring loop that pings active connections."""
        while True:
            await asyncio.sleep(30)  # Ping every 30 seconds

            # Only monitor connections that are actually open
            active_connections = [
                (url, conn) for url, conn in self.connections.items()
                if conn and hasattr(conn, "open") and conn.open
            ]

            for url, connection in active_connections:
                await self._ping_connection(url, connection)

    async def _ping_connection(
        self, url: WebsocketUrl, connection: ClientConnection
    ) -> None:
        """
        Ping a connection to measure real latency and update error metrics.
        Uses native WebSocket ping frames instead of HTTP health checks.
        This measures actual responsiveness of the endpoint for Ogmios operations.
        """
        try:
            start_time = time.perf_counter()
            pong_waiter = await connection.ping()
            await asyncio.wait_for(pong_waiter, timeout=5.0)
            end_time = time.perf_counter()
            latency_ms = (end_time - start_time) * 1000

            metrics = self.metrics.get(url, ConnectionMetrics())
            metrics.latency_ms = latency_ms
            metrics.last_ping = time.time()
            self.metrics[url] = metrics

            logger.debug(f"Ping {url}: {latency_ms:.2f}ms")

        except (asyncio.TimeoutError, ConnectionClosed) as e:
            logger.warning(f"Ping failed for {url}: {e}")
            await connection.close()
            self.connections[url] = None
            metrics = self.metrics.get(url, ConnectionMetrics())
            metrics.connection_errors += 1
            metrics.last_error_time = time.time()
            self.metrics[url] = metrics

    async def close_all_connections(self) -> None:
        """Close all open connections."""
        await self.stop_monitoring()

        for url, connection in self.connections.items():
            if connection and hasattr(connection, "open") and connection.open:
                await connection.close()
                logger.debug("Closed connection to %s", url)

        self.connections.clear()
        logger.info("All connections closed")
