import orjson as json
from typing import Any

from ogmios import Block, Point
from ogmios.client import Client as OgmiosClient
from ogmios.model.ogmios_model import Jsonrpc
from websockets import connect, ClientConnection

from client.chainsync import AsyncFindIntersection, AsyncNextBlock
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES

from client.ledgerstate import AsyncEraSummaries
from models import EpochNumber


class HecateClient(OgmiosClient):  # type: ignore[misc]
    """
    Async Ogmios connection client

    Asynchronous wrapper of the Ogmios websockets client.
    Inherits from OgmiosClient for type compatibility but implements
    its own async connection management as well as additional, higher-level methods.

    :param host: The host of the Ogmios server
    :param port: The port of the Ogmios server
    :param path: Optional path for the WebSocket connection
    :param secure: Use secure connection
    :param rpc_version: The JSON-RPC version to use
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1337,
        path: str = "",
        secure: bool = False,
        rpc_version: Jsonrpc = Jsonrpc.field_2_0,
    ) -> None:
        protocol: str = "wss" if secure else "ws"
        self.rpc_version = rpc_version
        self.connect_str: str = f"{protocol}://{host}:{port}/{path}"
        self.connection: ClientConnection | None = None

        # chainsync methods
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

        # ledgerstate methods
        self.era_summaries = AsyncEraSummaries(self)

    # Connection management
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client connection when finished"""
        await self.close()

    async def connect(self) -> None:
        """Connect to the Ogmios server"""
        if self.connection is None:
            self.connection = await connect(self.connect_str)

    async def close(self) -> None:
        """Close the connection to the Ogmios server"""
        if self.connection:
            await self.connection.close()

    async def send(self, request: str) -> None:
        """Send a request to the Ogmios server asynchronously

        :param request: The request to send
        :type request: str
        """
        if not self.connection:
            await self.connect()
            assert self.connection is not None

        await self.connection.send(request)

    async def receive(self) -> dict[str, Any]:
        """Receive a response from the Ogmios server

        :return: Request response
        """
        if self.connection is None:
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
    async def epoch_blocks(self, epoch: EpochNumber, request_id: Any = None) -> list[Block]:
        """
        Get blocks produced on the given epoch.
        Said epoch number must be greater than the last Byron epoch (207) and be finalized.
        This is meant to be used for historical data retrieval.
        :param epoch: The epoch to get blocks from
        :param request_id: The prefix to send in request IDs
        """
        epoch_boundaries = EPOCH_BOUNDARIES[epoch]
        intersection_point = Point(
            slot=epoch_boundaries.start_slot, id=epoch_boundaries.start_hash
        )
        intersection, tip, request_id = await self.find_intersection.execute(
            points=[intersection_point],
            request_id=request_id,
        )
        if intersection != intersection_point:
            raise ValueError(
                f"Couldn't interesect the start of epoch {epoch}, got: {intersection}"
            )

        # Once we have the intersection, we can start requesting blocks in batches
        expected_blocks = BLOCKS_IN_EPOCH[epoch]
        remaining_blocks = expected_blocks
        blocks: list[Block] = []
        while len(blocks) < expected_blocks:
            blocks += await self.next_block.batched(
                # Avoid unnecessary requests
                batch_size=min(remaining_blocks, self.next_block.batch_size)
            )
            remaining_blocks = expected_blocks - len(blocks)
        return blocks
