from typing import Any

import ogmios.model.ogmios_model as om
from ogmios import QueryEraSummaries

from client.base import AsyncOgmiosMethod


class AsyncEraSummaries(AsyncOgmiosMethod[tuple[list[om.EraSummary], Any | None]]):
    """
    Async wrapper for Ogmios QueryEraSummaries method.
    Uses composition with the original QueryEraSummaries class.
    """

    ogmios_class = QueryEraSummaries
    parser_method_name = "_parse_QueryEraSummaries_response"

    def _create_payload(
            self,
            request_id: Any = None,
            **kwargs: Any
    ) -> om.QueryLedgerStateEraSummaries:
        """Create the payload for the query_era_summaries request.

        :param request_id: The ID of the request.
        :return: The request payload.
        """
        return om.QueryLedgerStateEraSummaries(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            id=request_id,
        )
