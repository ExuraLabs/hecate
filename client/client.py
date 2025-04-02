from __future__ import annotations
import json
from typing import Dict, Any, Optional

import websockets.client

from ogmios.model.ogmios_model import Jsonrpc


class HecateClient:
    """
    Async Ogmios connection client

    Asynchronous implementation of the Ogmios client using websockets.

    :param host: The host of the Ogmios server
    :type host: str
    :param port: The port of the Ogmios server
    :type port: int
    :param path: Optional path for the WebSocket connection
    :type path: str
    :param secure: Use secure connection
    :type secure: bool
    :param rpc_version: The JSON-RPC version to use
    :type rpc_version: Jsonrpc
    :param additional_headers: Additional headers to include in the WebSocket connection
    :type additional_headers: dict
    """

    def __init__(
            self,
            host: str = "argon",
            port: int = 1337,
            path: str = "",
            secure: bool = False,
            rpc_version: Jsonrpc = Jsonrpc.field_2_0,
            additional_headers: Dict[str, str] = {},
    ) -> None:
        protocol: str = "wss" if secure else "ws"
        self.rpc_version = rpc_version
        self.connect_str: str = f"{protocol}://{host}:{port}/{path}"
        self.additional_headers = additional_headers
        self.connection: Optional[websockets.client.WebSocketClientProtocol] = None

        # Ogmios chainsync methods
        from client import AsyncFindIntersection
        from client import AsyncNextBlock
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client connection when finished"""
        await self.close()

    async def connect(self) -> None:
        """Connect to the Ogmios server"""
        if self.connection is None or self.connection.closed:
            self.connection = await websockets.client.connect(
                self.connect_str,
                extra_headers=self.additional_headers
            )

    async def close(self) -> None:
        """Close the connection to the Ogmios server"""
        if self.connection:
            await self.connection.close()

    async def send(self, request: str) -> None:
        """Send a request to the Ogmios server

        :param request: The request to send
        :type request: str
        """
        if not self.connection:
            await self.connect()

        await self.connection.send(request)

    async def receive(self) -> Dict[str, Any]:
        """Receive a response from the Ogmios server

        :return: Request response
        """
        if not self.connection:
            await self.connect()

        raw_response = await self.connection.recv()
        resp = json.loads(raw_response)
        if resp.get("version"):
            raise Exception(
                "Invalid Ogmios version. ogmios-python only supports Ogmios server version v6.0.0 and above."
            )
        return resp
