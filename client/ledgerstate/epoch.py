from typing import Any

import ogmios.model.ogmios_model as om
from ogmios import QueryEpoch

from client.base import AsyncOgmiosMethod


class AsyncEpoch(AsyncOgmiosMethod[tuple[int, Any | None]]):
    """
    Async wrapper for Ogmios QueryEpoch method.
    Uses composition with the original QueryEpoch class.
    """

    ogmios_class = QueryEpoch
    parser_method_name = "_parse_QueryEpoch_response"

    def _create_payload(
        self,
        request_id: Any = None,
        **kwargs: Any,
    ) -> om.QueryLedgerStateEpoch:
        """Create the payload for the query_epoch request.

        :param request_id: The ID of the request.
        :return: The request payload.
        """
        return om.QueryLedgerStateEpoch(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            id=request_id,
        )
