from typing import Any

import ogmios.model.ogmios_model as om
from ogmios import FindIntersection, Origin, Point, Tip

from client.base import AsyncOgmiosMethod


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
