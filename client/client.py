from typing import Any, AsyncIterator

from ogmios import Block, Point
from ogmios.client import Client as OgmiosClient
from ogmios.model.ogmios_model import Jsonrpc
import orjson as json
from websockets import connect, ClientConnection

from client.chainsync import AsyncFindIntersection, AsyncNextBlock
from client.ledgerstate import AsyncEpoch, AsyncEraSummaries, AsyncTip
from client.multi_source_balancer import MultiSourceBalancer
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES

from models import EpochNumber


class HecateClient(OgmiosClient):  # type: ignore[misc]
    """
    Async Ogmios connection client with multi-source load balancing.

    Asynchronous wrapper of the Ogmios websockets client.
    Inherits from OgmiosClient for type compatibility but implements
    its own async connection management as well as additional, higher-level methods.
    It uses a MultiSourceBalancer to distribute requests across multiple Ogmios instances.

    :param balancer: A MultiSourceBalancer instance. If not provided, it will be
        created from environment variables.
    :param rpc_version: The JSON-RPC version to use.
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        balancer: MultiSourceBalancer | None = None,
        rpc_version: Jsonrpc = Jsonrpc.field_2_0,
    ) -> None:
        self.rpc_version = rpc_version
        self.balancer = balancer or MultiSourceBalancer.from_env()
        self.connection: ClientConnection | None = None

        # chainsync methods
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

        # ledgerstate methods
        self.era_summaries = AsyncEraSummaries(self)
        self.chain_tip = AsyncTip(self)
        self.epoch = AsyncEpoch(self)

    # Connection management
    async def __aenter__(self) -> "HecateClient":
        self.balancer.start_health_checks()
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client connection and stop health checks when finished"""
        await self.balancer.stop_health_checks()
        await self.close()

    async def connect(self, **connection_params: Any) -> None:
        """Connect to the best available Ogmios server from the balancer."""
        if self.connection is not None:
            await self.connection.close()

        endpoint = self.balancer.get_best_endpoint()
        self.connection = await connect(str(endpoint.url), **connection_params)

    async def close(self) -> None:
        """Close the connection to the Ogmios server"""
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def send(self, request: str) -> None:
        """Send a request to the Ogmios server asynchronously.
        If the connection is lost, it will try to reconnect to the next best endpoint.

        :param request: The request to send
        :type request: str
        """
        try:
            if not self.connection or self.connection.closed:
                await self.connect()
            assert self.connection is not None
            await self.connection.send(request)
        except Exception:
            # Reconnect on failure
            await self.connect()
            assert self.connection is not None
            await self.connection.send(request)

    async def receive(self) -> dict[str, Any]:
        """Receive a response from the Ogmios server

        :return: Request response
        """
        if not self.connection or self.connection.closed:
            await self.connect()
        assert self.connection is not None

        raw_response = await self.connection.recv()
        resp: dict[str, Any] = json.loads(raw_response)
        if resp.get("version"):
            raise Exception(
                "Invalid Ogmios version. ogmios-python only supports Ogmios server version v6.0.0 and above."
            )
        return resp

    # Higher-level methods
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
