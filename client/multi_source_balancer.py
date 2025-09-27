import asyncio
import logging
import random
import time

import aiohttp
from pydantic import BaseModel, Field, WebsocketUrl

from config.settings import get_ogmios_settings

logger = logging.getLogger(__name__)


class OgmiosEndpoint(BaseModel):
    """Represents a single Ogmios endpoint with health and weight."""

    url: WebsocketUrl
    weight: float = Field(
        1.0, description="Weight for load balancing (higher is more preferred)."
    )
    is_healthy: bool | None = Field(
        None, description="Current health status of the endpoint."
    )
    latency_ms: float | None = Field(
        None, description="Last measured latency in milliseconds."
    )


class MultiSourceBalancer:
    """
    Manages and balances load across multiple Ogmios endpoints.
    """

    def __init__(
        self,
        endpoints: list[OgmiosEndpoint],
        health_check_interval: int = 60,
    ):
        if not endpoints:
            raise ValueError("At least one Ogmios endpoint must be provided.")
        self.endpoints = endpoints
        self.health_check_interval = health_check_interval
        self._health_check_task: asyncio.Task | None = None
        self._initial_check_done = asyncio.Event()

    @classmethod
    def from_env(cls) -> "MultiSourceBalancer":
        """
        Creates a balancer from centralized settings.
        """
        settings = get_ogmios_settings()
        endpoints_data = settings.endpoints
        endpoints = [OgmiosEndpoint(**data) for data in endpoints_data]
        return cls(endpoints)

    async def _perform_health_check_cycle(self) -> None:
        """Runs a single cycle of health checks for all endpoints."""
        async with aiohttp.ClientSession() as session:
            tasks = [self._check_health(session, ep) for ep in self.endpoints]
            await asyncio.gather(*tasks)

    async def _run_initial_health_check(self) -> None:
        """Runs a one-off health check for all endpoints and signals completion."""
        logger.info(
            f"🔍 Starting initial health check for {len(self.endpoints)} Ogmios endpoints..."
        )
        await self._perform_health_check_cycle()

        # Log summary of health check results
        healthy_count = sum(1 for ep in self.endpoints if ep.is_healthy)
        logger.info(
            f"✅ Initial Ogmios endpoint health check complete: "
            f"{healthy_count}/{len(self.endpoints)} endpoints healthy"
        )

        if healthy_count == 0:
            logger.error(
                "⚠️ WARNING: No healthy endpoints found! Check your Ogmios servers."
            )

        self._initial_check_done.set()

    async def _check_health(
        self, session: aiohttp.ClientSession, endpoint: OgmiosEndpoint
    ) -> None:
        """Performs a health check on a single endpoint."""
        # Convert WebSocket URL to HTTP URL and ensure proper formatting
        http_url = (
            str(endpoint.url).replace("ws://", "http://").replace("wss://", "https://")
        )
        # Remove trailing slash to avoid double slashes
        http_url = http_url.rstrip("/")
        health_url = f"{http_url}/health"

        try:
            start_time = time.perf_counter()
            timeout = aiohttp.ClientTimeout(
                total=10
            )  # Increased timeout for better reliability
            async with session.get(health_url, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()
                network_sync = data.get("networkSynchronization", 0)
                is_healthy = network_sync > 0.99
                end_time = time.perf_counter()

                if is_healthy:
                    endpoint.is_healthy = True
                    endpoint.latency_ms = (end_time - start_time) * 1000
                    logger.debug(
                        f"✅ Endpoint {endpoint.url} is healthy "
                        f"(sync: {network_sync:.4f}, latency: {endpoint.latency_ms:.2f}ms)"
                    )
                else:
                    endpoint.is_healthy = False
                    endpoint.latency_ms = None
                    logger.warning(
                        f"⚠️ Endpoint {endpoint.url} is unhealthy "
                        f"(sync: {network_sync:.4f} < 0.99 required)"
                    )

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # Don't automatically mark as unhealthy for HTTP health check failures
            # The WebSocket might still work (as we've seen with radon)
            endpoint.latency_ms = None
            logger.warning(
                f"⚠️ HTTP health check failed for {endpoint.url} (trying {health_url}): {type(e).__name__}: {e}"
            )
            logger.info(
                f"🔄 Note: WebSocket connection might still work for {endpoint.url}"
            )
        except Exception as e:
            endpoint.is_healthy = False
            endpoint.latency_ms = None
            logger.exception(
                f"💥 Unexpected error during health check for {endpoint.url}: {e}"
            )

    async def _run_health_checks(self) -> None:
        """Periodically runs health checks for all endpoints."""
        while True:
            await self._perform_health_check_cycle()
            await asyncio.sleep(self.health_check_interval)

    async def run_initial_health_check(self) -> None:
        """
        Public method to run the initial health check.
        This should be called before using get_best_endpoint().
        """
        await self._run_initial_health_check()

    async def start_health_checks(self) -> None:
        """Starts the background health check task and runs initial check if not done."""
        # CRITICAL: Ensure initial check is ALWAYS completed before returning
        # This guarantees that get_best_endpoint() will have valid health data
        if not self._initial_check_done.is_set():
            logger.info(
                "Running initial Ogmios health check before starting periodic checks..."
            )
            await self.run_initial_health_check()

        # Only start the periodic task if it's not already running
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(self._run_health_checks())
            logger.info("Ogmios multi-source health checker started successfully.")

    async def stop_health_checks(self) -> None:
        """Stops the background health check task."""
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            logger.info("Ogmios multi-source health checker stopped.")

    async def get_best_endpoint(self) -> OgmiosEndpoint:
        """
        Selects the best endpoint based on health, latency, and weight.
        Waits for the initial health check to complete before selecting.
        """
        # Wait for initial health check with a timeout for better error handling
        try:
            await asyncio.wait_for(self._initial_check_done.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("Initial health check timed out after 30 seconds!")
            # Continue anyway, but with a warning

        healthy_endpoints = [ep for ep in self.endpoints if ep.is_healthy]

        # If no endpoints passed HTTP health check, still try all endpoints
        # because WebSocket might work even if HTTP health check fails
        if not healthy_endpoints:
            logger.info(
                f"No endpoints passed HTTP health checks, but will try all {len(self.endpoints)} "
                f"endpoints as WebSocket connections might still work"
            )
            available_endpoints = self.endpoints
        else:
            logger.debug(
                f"Found {len(healthy_endpoints)} healthy endpoints out of {len(self.endpoints)}"
            )
            available_endpoints = healthy_endpoints

        # Simple weighted random selection for now.
        # A more sophisticated strategy could consider latency.
        total_weight = sum(ep.weight for ep in available_endpoints)
        if total_weight == 0:
            selected = random.choice(available_endpoints)
            logger.debug(f"Selected random endpoint (zero weights): {selected.url}")
            return selected

        selection = random.uniform(0, total_weight)
        current_weight = 0
        for endpoint in available_endpoints:
            current_weight += endpoint.weight
            if current_weight >= selection:
                latency_str = (
                    f"{endpoint.latency_ms:.2f}ms"
                    if endpoint.latency_ms is not None
                    else "unknown"
                )
                logger.debug(
                    f"Selected Ogmios endpoint: {endpoint.url} "
                    f"(healthy: {endpoint.is_healthy}, latency: {latency_str}, weight: {endpoint.weight})"
                )
                return endpoint

        # Fallback in case of floating point inaccuracies
        fallback = random.choice(available_endpoints)
        logger.debug(
            f"Using fallback endpoint due to floating point precision: {fallback.url}"
        )
        return fallback
