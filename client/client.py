import logging
import os
from typing import Any, AsyncIterator

from ogmios import Block, Point
from ogmios.client import Client as OgmiosClient
from ogmios.model.ogmios_model import Jsonrpc
import orjson as json
from websockets import connect, ClientConnection

from client.chainsync import AsyncFindIntersection, AsyncNextBlock
from client.ledgerstate.tip import AsyncTip
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES

from client.ledgerstate import AsyncEraSummaries
from models import EpochNumber


class HecateClient(OgmiosClient):  # type: ignore[misc]
    """
    Async Ogmios connection client

    Asynchronous wrapper of the Ogmios websockets client.
    Inherits from OgmiosClient for type compatibility but implements
    its own async connection management as well as additional, higher-level methods.

    :param host: The host of the Ogmios server.
        If not provided, defaults the value of the environment variable ``OGMIOS_HOST`` if set,
        or ``localhost`` otherwise.
    :param port: The port of the Ogmios server.
        If not provided, defaults to 1337, unless the environment variable ``OGMIOS_PORT`` is set,
        in which case it will use that value instead.
    :param path: Optional path for the WebSocket connection
    :param secure: Use secure connection
    :param rpc_version: The JSON-RPC version to use
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        host: str | None = None,
        port: int = 1337,
        path: str = "",
        secure: bool = False,
        rpc_version: Jsonrpc = Jsonrpc.field_2_0,
    ) -> None:
        self.rpc_version = rpc_version
        self.connect_str: str = self.get_connection_url(host, port, path, secure)
        self.connection: ClientConnection | None = None

        # chainsync methods
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

        # ledgerstate methods
        self.era_summaries = AsyncEraSummaries(self)
        self.chain_tip = AsyncTip(self)

    # Connection management
    async def __aenter__(self) -> "HecateClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client connection when finished"""
        await self.close()

    @staticmethod
    def get_connection_url(
        host: str | None = None,
        port: int = 1337,
        path: str = "",
        secure: bool = False,
    ) -> str:
        """
        Generate a WebSocket connection URLto interact with an Ogmios instance.

        Constructs a connection URL using the provided parameters. It also supports
        using environment variables for the host and port configuration if they are not explicitly
        provided. The connection protocol can be set to secure (wss) or non-secure (ws), and a
        specific path can be appended to the constructed URL.

        :param host: The hostname of the server. If not provided, it defaults to the
            value of the environment variable ``OGMIOS_HOST`` if set, or ``localhost`` otherwise.
        :param port: The port number to connect to. Defaults to 1337. If the
            environment variable ``OGMIOS_PORT`` is set, its value will be used instead.
        :param path: The URL path to append to the connection string. Defaults to an
            empty string.
        :param secure: A boolean flag indicating whether to use a secure WebSocket
            connection (wss) or a non-secure one (ws). Defaults to ``False``.
        :return: A constructed WebSocket connection URL as a string.
        :rtype: str
        """
        _OGMIOS_HOST = os.getenv("OGMIOS_HOST")
        if host is None and _OGMIOS_HOST:
            host = _OGMIOS_HOST
        else:
            host = host or "localhost"

        _OGMIOS_PORT = os.getenv("OGMIOS_PORT")
        if _OGMIOS_PORT:
            port = int(_OGMIOS_PORT)

        protocol: str = "wss" if secure else "ws"
        connect_str = f"{protocol}://{host}:{port}/{path}"
        return connect_str

    async def connect(self, **connection_params: Any) -> None:
        """Connect to the Ogmios server"""
        if self.connection is None:
            self.connection = await connect(self.connect_str, **connection_params)

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
    async def epoch_blocks(
        self, epoch: EpochNumber, request_id: Any = None
    ) -> AsyncIterator[list[Block]]:
        """
        Get blocks produced on the given epoch.
        Epoch number must be greater than the last Byron epoch (207) and be finalized.
        This is meant to be used for historical data retrieval.
        Yield sub-batches as soon as they arrive, pipelining up to next_block.batch_size requests.
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
        # But first! We need to set the logger level to ERROR
        # to supress pydantic's spurious warnings about schema validation
        ogmios_logger = logging.getLogger("ogmios")
        ogmios_logger.setLevel(logging.ERROR)

        while remaining_blocks > 0:
            batch = await self.next_block.batched(
                # Avoid unnecessary requests
                batch_size=min(remaining_blocks, self.next_block.batch_size)
            )
            remaining_blocks -= len(batch)
            yield batch
