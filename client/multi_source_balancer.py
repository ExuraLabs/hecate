import asyncio
import logging
import os
import random
from typing import List, Optional

import aiohttp
from pydantic import BaseModel, Field, HttpUrl

from config.settings import get_ogmios_settings

logger = logging.getLogger(__name__)


class OgmiosEndpoint(BaseModel):
    """Represents a single Ogmios endpoint with health and weight."""

    url: HttpUrl
    weight: float = Field(
        1.0, description="Weight for load balancing (higher is more preferred)."
    )
    is_healthy: bool = Field(True, description="Current health status of the endpoint.")
    latency_ms: float = Field(
        -1.0, description="Last measured latency in milliseconds."
    )


class MultiSourceBalancer:
    """
    Manages and balances load across multiple Ogmios endpoints.
    """

    def __init__(
        self, endpoints: List[OgmiosEndpoint], health_check_interval: int = 60
    ):
        if not endpoints:
            raise ValueError("At least one Ogmios endpoint must be provided.")
        self.endpoints = endpoints
        self.health_check_interval = health_check_interval
        self._health_check_task: Optional[asyncio.Task] = None

    @classmethod
    def from_env(cls) -> "MultiSourceBalancer":
        """Creates a balancer from centralized settings."""
        settings = get_ogmios_settings()
        endpoints_data = settings.endpoints
        endpoints = [OgmiosEndpoint(**data) for data in endpoints_data]
        return cls(endpoints)

    async def _check_health(self, session: aiohttp.ClientSession, endpoint: OgmiosEndpoint) -> None:
        """Performs a health check on a single endpoint."""
        try:
            start_time = asyncio.get_event_loop().time()
            async with session.get(f"{endpoint.url}/health", timeout=5) as response:
                response.raise_for_status()
                data = await response.json()
                is_healthy = data.get("networkSynchronization", 0) > 0.99
                end_time = asyncio.get_event_loop().time()

                if is_healthy:
                    endpoint.is_healthy = True
                    endpoint.latency_ms = (end_time - start_time) * 1000
                    logger.debug(f"Endpoint {endpoint.url} is healthy (latency: {endpoint.latency_ms:.2f}ms).")
                else:
                    endpoint.is_healthy = False
                    logger.warning(f"Endpoint {endpoint.url} is unhealthy (sync: {data.get('networkSynchronization')}).")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            endpoint.is_healthy = False
            logger.error(f"Health check failed for {endpoint.url}: {e}")
        except Exception:
            endpoint.is_healthy = False
            logger.exception(f"Unexpected error during health check for {endpoint.url}")

    async def _run_health_checks(self) -> None:
        """Periodically runs health checks for all endpoints."""
        async with aiohttp.ClientSession() as session:
            while True:
                tasks = [self._check_health(session, ep) for ep in self.endpoints]
                await asyncio.gather(*tasks)
                await asyncio.sleep(self.health_check_interval)

    def start_health_checks(self) -> None:
        """Starts the background health check task."""
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(self._run_health_checks())
            logger.info("Ogmios multi-source health checker started.")

    async def stop_health_checks(self) -> None:
        """Stops the background health check task."""
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            logger.info("Ogmios multi-source health checker stopped.")

    def get_best_endpoint(self) -> OgmiosEndpoint:
        """
        Selects the best endpoint based on health, latency, and weight.
        """
        healthy_endpoints = [ep for ep in self.endpoints if ep.is_healthy]

        if not healthy_endpoints:
            logger.error("No healthy Ogmios endpoints available! Falling back to all endpoints.")
            # Fallback to using any endpoint if all are unhealthy
            healthy_endpoints = self.endpoints

        # Simple weighted random selection for now.
        # A more sophisticated strategy could consider latency.
        total_weight = sum(ep.weight for ep in healthy_endpoints)
        if total_weight == 0:
            return random.choice(healthy_endpoints)

        selection = random.uniform(0, total_weight)
        current_weight = 0
        for endpoint in healthy_endpoints:
            current_weight += endpoint.weight
            if current_weight >= selection:
                logger.debug(f"Selected Ogmios endpoint: {endpoint.url}")
                return endpoint
        
        # Fallback in case of floating point inaccuracies
        return random.choice(healthy_endpoints)
