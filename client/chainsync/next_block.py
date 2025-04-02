from typing import Any, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .client import HecateClient
from client import HecateClient

from ogmios.errors import InvalidMethodError, InvalidResponseError
from ogmios.logger import logger
from ogmios.datatypes import Origin, Point, Tip, Block, Direction
import ogmios.response_handler as rh
import ogmios.model.ogmios_model as om
import ogmios.model.model_map as mm


class AsyncNextBlock:
    """Async Ogmios method to request the next block in the blockchain.

    :param client: The client to use for the request.
    :type client: HecateClient
    """

    def __init__(self, client: HecateClient):
        self.client = client
        self.method = mm.Method.nextBlock.value

    async def execute(
        self, id: Optional[Any] = None
    ) -> Tuple[Direction, Tip, Union[Point, Origin, Block], Optional[Any]]:
        """Send and receive the request.

        :param id: The ID of the request.
        :type id: Any
        :return: The direction, tip, point or block or origin, and ID of the response.
        :rtype: (Direction, Tip, Point | Origin | Block, Optional[Any])
        """
        await self.send(id)
        return await self.receive()

    async def send(self, id: Optional[Any] = None) -> None:
        """Send the request.

        :param id: The ID of the request.
        :type id: Any
        """
        pld = om.NextBlock(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            id=id,
        )
        await self.client.send(pld.json())

    async def receive(self) -> Tuple[Direction, Tip, Union[Point, Origin, Block], Optional[Any]]:
        """Receive a previously requested response.

        :return: The direction, tip, point or block or origin, and ID of the response.
        :rtype: (Direction, Tip, Point | Origin | Block, Optional[Any])
        """
        response = await self.client.receive()
        return self._parse_NextBlock_response(response)

    @staticmethod
    def _parse_NextBlock_response(
        response: dict,
    ) -> Tuple[Direction, Tip, Union[Point, Origin, Block], Optional[Any]]:
        if response.get("method") != mm.Method.nextBlock.value:
            raise InvalidMethodError(f"Incorrect method for next_block response: {response}")

        # Successful response will contain direction, tip, and either a block or point
        if result := response.get("result"):
            if result.get("direction") and result.get("tip"):
                direction: Direction = AsyncNextBlock._parse_direction(result.get("direction"))
                tip: Union[Tip, Origin] = rh.parse_TipOrOrigin(result.get("tip"))
                id: Optional[Any] = response.get("id")
                if (block_resp := result.get("block")) is not None:
                    block: Block = rh.parse_Block(block_resp)
                    logger.info(
                        f"""Parsed NextBlock response:
        Direction = {direction}
        Tip = {tip}
        Block = {block}
        ID = {id}"""
                    )
                    return direction, tip, block, id
                elif (point_resp := result.get("point")) is not None:
                    point: Union[Point, Origin] = rh.parse_PointOrOrigin(point_resp)
                    logger.info(
                        f"""Parsed NextBlock response:
        Direction = {direction}
        Tip = {tip}
        Point = {point}
        ID = {id}"""
                    )
                    return direction, tip, point, id
        raise InvalidResponseError(f"Failed to parse next_block response: {response}")

    @staticmethod
    def _parse_direction(value: str) -> Direction:
        if value == Direction.forward.value:
            return Direction.forward
        elif value == Direction.backward.value:
            return Direction.backward
        raise InvalidResponseError(
            f"next_block response contains invalid direction parameter: {value}"
        )
