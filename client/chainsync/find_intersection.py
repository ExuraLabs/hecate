from __future__ import annotations

from typing import Any, List, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from client import HecateClient
from client import HecateClient

from ogmios.errors import InvalidMethodError, InvalidResponseError, ResponseError
from ogmios.logger import logger
from ogmios.datatypes import Origin, Point, Tip
import ogmios.response_handler as rh
import ogmios.model.ogmios_model as om
import ogmios.model.model_map as mm


class AsyncFindIntersection:
    """Async Ogmios method to find a point on the blockchain. The first point that is found in the
    provided list will be returned.

    :param client: The client to use for the request.
    :type client: AsyncClient
    """

    def __init__(self, client: HecateClient):
        self.client = client
        self.method = mm.Method.findIntersection.value

    async def execute(
        self, points: List[Union[Point, Origin]], id: Optional[Any] = None
    ) -> Tuple[Union[Point, Origin], Union[Tip, Origin], Optional[Any]]:
        """Send and receive the request.

        :param points: The list of points to find.
        :type points: List[Union[Point, Origin]]
        :param id: The ID of the request.
        :type id: Any
        :return: The intersection, tip, and ID of the response.
        :rtype: Tuple[Union[Point, Origin], Union[Tip, Origin], Optional[Any]]
        """
        await self.send(points, id)
        return await self.receive()

    async def send(self, points: List[Union[Point, Origin]], id: Optional[Any] = None) -> None:
        """Send the request.

        :param points: The list of points to find.
        :type points: List[Union[Point, Origin]]
        :param id: The ID of the request.
        :type id: Any
        """
        params = om.Params(points=[point._schematype for point in points])
        pld = om.FindIntersection(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            params=params,
            id=id,
        )
        await self.client.send(pld.json())

    async def receive(self) -> Tuple[Union[Point, Origin], Union[Tip, Origin], Optional[Any]]:
        """Receive the response.

        :return: The intersection, tip, and ID of the response.
        :rtype: Tuple[Union[Point, Origin], Union[Tip, Origin], Optional[Any]]
        """
        response = await self.client.receive()
        return self._parse_FindIntersection_response(response)

    @staticmethod
    def _parse_FindIntersection_response(
        response: dict,
    ) -> Tuple[Union[Point, Origin], Union[Tip, Origin], Optional[Any]]:
        if response.get("method") != mm.Method.findIntersection.value:
            raise InvalidMethodError(f"Incorrect method for find_intersection response: {response}")

        if response.get("error"):
            raise ResponseError(f"Ogmios responded with error: {response}")

        # Successful response will contain intersection, tip, and id
        if result := response.get("result"):
            if (intersection_resp := result.get("intersection")) is not None and (
                tip_resp := result.get("tip")
            ) is not None:
                intersection: Union[Point, Origin] = rh.parse_PointOrOrigin(intersection_resp)
                tip: Union[Tip, Origin] = rh.parse_TipOrOrigin(tip_resp)
                id: Optional[Any] = response.get("id")
                logger.info(
                    f"""Parsed find_intersection response:
        Point = {intersection}
        Tip = {tip}
        ID = {id}"""
                )
                return intersection, tip, id
        raise InvalidResponseError(f"Failed to parse find_intersection response: {response}")
