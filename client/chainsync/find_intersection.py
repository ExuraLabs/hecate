from collections.abc import Sequence
from typing import Any

import ogmios.model.ogmios_model as om
import ogmios.response_handler as rh
from ogmios import FindIntersection, Origin, Point, Tip

from client.base import AsyncOgmiosMethod

#: Ogmios v6's JSON-RPC error code for "none of the points are on my chain".
INTERSECTION_NOT_FOUND = 1000


class AsyncFindIntersection(
    AsyncOgmiosMethod[tuple[Point | Origin, Tip | Origin, Any | None]]
):
    """
    Async wrapper for Ogmios FindIntersection method.
    Uses composition with the original FindIntersection class.
    """

    ogmios_class = FindIntersection
    parser_method_name = "_parse_FindIntersection_response"

    def _create_payload(
        self, request_id: Any = None, **kwargs: Any
    ) -> om.FindIntersection:
        """Create the payload for the find_intersection request.

        :param request_id: The ID of the request.
        :type request_id: Any
        :param points: The list of points to find.
        :type points: list[Point | Origin]
        :return: The request payload.
        """
        points = kwargs.get("points", [])
        params = om.Params(points=[point._schematype for point in points])
        return om.FindIntersection(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            params=params,
            id=request_id,
        )

    async def locate(self, points: Sequence[Point]) -> tuple[Point | None, Tip]:
        """Intersect at the first of ``points`` the node holds, newest first.

        Unlike ``execute``, "none of them" is an answer rather than an error:
        it comes back as ``None`` alongside the node's tip, because whether
        that is fatal depends on where the tip is — a node still syncing has
        simply not reached the points yet.

        :param points: Candidate points, most recent first.
        :return: The intersection (or None) and the node's tip.
        :raises ValueError: if the node answers with Origin for either; a
            relay has no use for an empty chain.
        """
        await self.send(points=list(points))
        response = await self.client.receive()

        error = response.get("error")
        if error is not None and error["code"] == INTERSECTION_NOT_FOUND:
            return None, _require_tip(rh.parse_TipOrOrigin(error["data"]["tip"]))

        intersection, tip, _ = self._parse_response(response)
        if not isinstance(intersection, Point):
            raise ValueError(f"intersected at {intersection}, not at a block")
        return intersection, _require_tip(tip)


def _require_tip(tip: Tip | Origin) -> Tip:
    if not isinstance(tip, Tip):
        raise ValueError("the node's tip is the origin: it holds no blocks yet")
    return tip
