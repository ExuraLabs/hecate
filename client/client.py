from __future__ import annotations

import logging
from typing import Any, AsyncIterator

import orjson as json
from ogmios import Block, Point
from ogmios.client import Client as OgmiosClient
from ogmios.model.ogmios_model import Jsonrpc
from websockets import ClientConnection, connect as ws_connect
from websockets.protocol import State

from client.chainsync import AsyncFindIntersection, AsyncNextBlock
from client.ledgerstate import AsyncEpoch, AsyncEraSummaries, AsyncTip
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES
from models import EpochNumber

logger = logging.getLogger(__name__)


class HecateClient(OgmiosClient):  # type: ignore[misc]
    """
    Async Ogmios connection client.

    Asynchronous wrapper of the Ogmios websockets client that inherits from OgmiosClient
    for type compatibility while implementing custom async connection management and
    additional higher-level query methods.

    Args:
        host: Hostname to connect to.
        port: Port to connect to.
        path: WebSocket path. Defaults to "".
        secure: Whether to use a secure (wss://) connection. Defaults to False.
        rpc_version: JSON-RPC version to use. Defaults to 2.0.
        endpoint_url: Full WebSocket URL (e.g., ws://localhost:1337).
                      When provided, overrides host/port/secure.
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        host: str | None = None,
        port: str | None = None,
        path: str = "",
        secure: bool = False,
        rpc_version: Jsonrpc = Jsonrpc.field_2_0,
        *,
        endpoint_url: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.path = path
        self.secure = secure
        self.rpc_version = rpc_version
        self.connection: ClientConnection | None = None
        self.endpoint_url = endpoint_url

        # chainsync methods
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

        # ledgerstate methods
        self.era_summaries = AsyncEraSummaries(self)
        self.chain_tip = AsyncTip(self)
        self.epoch = AsyncEpoch(self)

    async def __aenter__(self) -> HecateClient:
        if not self.connection:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client connection when finished"""
        await self.close()

    async def connect(self, **connection_params: Any) -> None:
        """Connect to the Ogmios server"""
        if self.connection is not None:
            return  # Already connected, noop

        # Use endpoint_url if provided, otherwise build from host/port/secure
        if self.endpoint_url:
            connection_url = self.endpoint_url
        else:
            if not self.host:
                raise ValueError("Host must be specified to build a connection URL")
            scheme = "wss" if self.secure else "ws"
            connection_url = f"{scheme}://{self.host}:{self.port or 1337}{self.path}"

        self.connection = await ws_connect(connection_url)

    async def close(self) -> None:
        if self.connection is None:
            return  # Already closed, noop

        await self.connection.close()
        self.connection = None

    async def send(self, request: str) -> None:
        """Send a request to the Ogmios server."""
        if not self.connection or self.connection.state != State.OPEN:
            raise RuntimeError("No connection available")

        await self.connection.send(request)

    async def receive(self) -> dict[str, Any]:
        """Receive a response from the Ogmios server."""
        if not self.connection or self.connection.state != State.OPEN:
            raise RuntimeError("No connection available")

        raw_response = await self.connection.recv()
        resp: dict[str, Any] = json.loads(raw_response)

        if resp.get("version"):
            raise Exception(
                "Invalid Ogmios version. The ogmios-python client only supports "
                "Ogmios server version v6.0.0 and above."
            )
        return resp

    async def epoch_blocks(
        self, epoch: EpochNumber, request_id: Any = None
    ) -> AsyncIterator[list[Block]]:
        """
        Get blocks produced on the given epoch.
        Epoch number must be greater than the last Byron epoch (207) and be finalized.
        This is meant to be used for historical data retrieval.
        Yield sub-batches as soon as they arrive, pipelining up to next_block.batch_size requests.
        The blocks are guaranteed to be unique and sorted by height in ascending order.
        :param epoch: The epoch to get blocks from
        :param request_id: The prefix to send in request IDs
        """
        # Intersect the end of the previous epoch so we can start streaming the expected one
        previous_epoch = EPOCH_BOUNDARIES[EpochNumber(epoch - 1)]
        intersection_point = Point(
            slot=previous_epoch.end_slot, id=previous_epoch.end_hash
        )
        intersection, tip, request_id = await self.find_intersection.execute(
            points=[intersection_point],
            request_id=request_id,
        )
        if intersection != intersection_point:
            raise ValueError(
                f"Couldn't intersect the start of epoch {epoch}, got: {intersection}"
            )

        # If we got here, the epoch blocks are available, so we can start fetching them
        remaining_blocks = BLOCKS_IN_EPOCH[epoch]
        expected_height = None
        last_height = None
        while remaining_blocks > 0:
            batch = await self.next_block.batched(
                # Avoid unnecessary requests
                batch_size=min(remaining_blocks, self.next_block.batch_size)
            )

            # Filter out duplicates. Since uniqueness is guaranteed within batches,
            # we can just check the last block of the previous batch against the first
            # of the current and remove it if it has the same height.
            if expected_height and batch[0].height == last_height:
                batch.pop(0)

            assert batch, (
                f"Received empty batch, expected {remaining_blocks} more blocks"
            )

            # Update the expected height for the next batch
            last_height = batch[-1].height
            expected_height = last_height + 1

            remaining_blocks -= len(batch)
            yield batch
