import asyncio
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

    def __init__(
        self,
        client: Any,
    ) -> None:
        """
        Initialize the AsyncNextBlock class.
        :param client: The client to use for sending requests.
        :param method: The method name to use for the request.
        :param request_id: The ID of the request.
        """
        from ogmios.model.cardano_model import Era2

        # Patch the Era2 class to handle the missing "conway" era
        Era2._missing_ = classmethod(lambda _cls, _value: _cls.babbage)
        super().__init__(client)

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
        reaching the tip, but rather for retrieving historical blocks. The returned blocks are
        guaranteed to be sorted by height in ascending order.

        Note: Any received duplicates or non-blocks (i.e.: Points) are filtered out, so
        the returned list may be shorter than `batch_size`.

        :param batch_size: Number of blocks to retrieve (defaults to self.batch_size)
        :return: A list of unique Block objects sorted by height in ascending order.
        """
        blocks = []
        if batch_size is None:
            batch_size = self.batch_size

        # Send all requests first
        for _ in range(batch_size):
            await self.send()

        # Collect all received blocks
        seen = set()
        for _ in range(batch_size):
            _, _, received, _ = await asyncio.wait_for(self.receive(), timeout=30.0)

            if not isinstance(received, Block) or received.height in seen:
                continue
            blocks.append(received)
            seen.add(received.height)

        # Sort blocks by height to ensure they're in the correct order
        blocks.sort(key=lambda block: block.height)

        return blocks
