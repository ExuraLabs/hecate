from typing import Any, AsyncIterator
import logging

from ogmios import Block, Point
from ogmios.client import Client as OgmiosClient
from ogmios.model.ogmios_model import Jsonrpc
import orjson as json
from websockets import ClientConnection
from pydantic import WebsocketUrl

from client.chainsync import AsyncFindIntersection, AsyncNextBlock
from client.ledgerstate import AsyncEpoch, AsyncEraSummaries, AsyncTip
from network.endpoint_scout import EndpointScout
from config.settings import get_ogmios_settings
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES

from models import EpochNumber

logger = logging.getLogger(__name__)


class HecateClient(OgmiosClient):  # type: ignore[misc]
    """
    Async Ogmios connection client with EndpointScout for intelligent connection management.

    Asynchronous wrapper of the Ogmios websockets client.
    Inherits from OgmiosClient for type compatibility but implements
    its own async connection management as well as additional, higher-level methods.
    It uses an EndpointScout to intelligently manage WebSocket connections to multiple Ogmios instances.

    :param endpoint_scout: An EndpointScout instance for connection management. If not provided,
        it will be created from environment variables.
    :param rpc_version: The JSON-RPC version to use.
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        endpoint_scout: EndpointScout | None = None,
        rpc_version: Jsonrpc = Jsonrpc.field_2_0,
    ) -> None:
        self.rpc_version = rpc_version
        self.endpoint_scout = endpoint_scout or self._create_default_scout()
        self.connection: ClientConnection | None = None

        # chainsync methods
        self.find_intersection = AsyncFindIntersection(self)
        self.next_block = AsyncNextBlock(self)

        # ledgerstate methods
        self.era_summaries = AsyncEraSummaries(self)
        self.chain_tip = AsyncTip(self)
        self.epoch = AsyncEpoch(self)

    def _create_default_scout(self) -> EndpointScout:
        """Crea un scout con configuración por defecto desde variables de entorno."""
        settings = get_ogmios_settings()
        endpoints = [WebsocketUrl(ep["url"]) for ep in settings.endpoints]
        return EndpointScout(endpoints)

    # Connection management
    async def __aenter__(self) -> "HecateClient":
        """Inicia el scout y establece conexión al entrar en el context manager."""
        await self.endpoint_scout.start_monitoring()
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cierra conexión y detiene monitoreo al salir del context manager."""
        await self.close()
        await self.endpoint_scout.stop_monitoring()

    async def connect(self, **connection_params: Any) -> None:
        """Establece conexión usando el endpoint scout."""
        if (
            self.connection
            and hasattr(self.connection, "open")
            and self.connection.open
        ):
            return

        try:
            self.connection = await self.endpoint_scout.get_best_connection()
            logger.info("✅ Connected to Ogmios endpoint via EndpointScout")
        except ConnectionError as e:
            logger.error(f"❌ Failed to establish connection: {e}")
            raise

    async def close(self) -> None:
        """Cierra la conexión al servidor Ogmios."""
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def shutdown(self) -> None:
        """Apaga el cliente completamente, deteniendo monitoreo y cerrando conexiones."""
        await self.endpoint_scout.close_all_connections()
        await self.close()

    async def send(self, request: str) -> None:
        """Envía una petición al servidor Ogmios de forma asíncrona.
        Si la conexión se pierde, intentará reconectar al mejor endpoint disponible.

        :param request: La petición a enviar
        :type request: str
        """
        try:
            if not self.connection:
                await self.connect()
                assert self.connection is not None
            await self.connection.send(request)
        except Exception:
            # Reconectar en caso de fallo
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
