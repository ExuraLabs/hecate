from typing import Any

from ogmios import Point, QueryLedgerTip
import ogmios.model.ogmios_model as om

from client.base import AsyncOgmiosMethod


class AsyncTip(AsyncOgmiosMethod[tuple[Point, Any | None]]):
    ogmios_class = QueryLedgerTip
    parser_method_name = "_parse_QueryLedgerTip_response"

    def _create_payload(
        self, request_id: Any = None, **kwargs: Any
    ) -> om.QueryLedgerStateTip:
        """
        Create the payload for the QueryLedgerTip request.
        :param request_id: The ID of the request.
        :return: The request payload.
        """
        return om.QueryLedgerStateTip(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            id=request_id,
        )
