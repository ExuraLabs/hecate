from typing import Any

import ogmios.model.ogmios_model as om
from ogmios import Block, Direction, NextBlock, Origin, Point, Tip

from client.base import AsyncOgmiosMethod


class AsyncNextBlock(
    AsyncOgmiosMethod[tuple[Direction, Tip, Point | Origin | Block, Any | None]]
):
    """
    Async wrapper for Ogmios NextBlock method.
    Uses composition with the original NextBlock class.
    """

    ogmios_class = NextBlock
    parser_method_name = "_parse_NextBlock_response"
    batch_size: int = 250

    def _create_payload(self, request_id: Any = None, **kwargs: Any) -> om.NextBlock:
        """
        Create the payload for the next_block request.
        :param request_id: The ID of the request.
        :return: The request payload.
        """
        return om.NextBlock(
            jsonrpc=self.client.rpc_version,
            method=self.method,
            id=request_id,
        )

    async def batched(self, batch_size: int | None = None) -> list[Block]:
        """
        Get the next blocks from the server. Assumes the cursor is at the desired intersection.
        This method sends a batch of requests and receives `batch_size` blocks. Not intended for
        reaching the tip, but rather for retrieving historical blocks.
        :param batch_size: Number of blocks to retrieve (defaults to self.batch_size)
        :return: A list of Block objects.
        """
        blocks = []
        if batch_size is None:
            batch_size = self.batch_size
        for _ in range(batch_size):
            await self.send()
        received_point_first = False
        for _ in range(batch_size):
            _, _, received, _ = await self.receive()
            if not isinstance(received, Block):
                # If received isn't a Block, means we have just started traversing the chain
                # In this case, flag that we have to make an extra request to reach batch_size.
                received_point_first = True
                continue
            blocks.append(received)
        if received_point_first:
            _, _, received, _ = await self.execute()
            blocks.append(received)
        return blocks
