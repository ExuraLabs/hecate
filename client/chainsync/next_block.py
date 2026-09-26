import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import ogmios.model.ogmios_model as om
import ogmios.response_handler as rh
from ogmios import Block, Direction, NextBlock, Origin, Point, Tip

from client.base import AsyncOgmiosMethod
from constants import SECURITY_PARAMETER, STAKE_CREDENTIAL_DEPOSIT
from errors import ChainLinkError

logger = logging.getLogger(__name__)


def _keep_through(held: OrderedDict[str, Point], point: Point) -> None:
    """Forget every point held after ``point``: the chain no longer has them."""
    if point.id not in held:
        held.clear()
        held[point.id] = point
        return
    while next(reversed(held)) != point.id:
        held.popitem()


@dataclass(frozen=True, slots=True)
class RollForward:
    """The node extended the chain by ``block``; ``tip`` is its tip as of this reply."""

    block: Block
    tip: Tip


@dataclass(frozen=True, slots=True)
class RollBackward:
    """Everything after ``point`` is gone from the node's chain."""

    point: Point
    tip: Tip


_CERT_TYPES_NEEDING_DEPOSIT = frozenset(
    {
        "stakeCredentialRegistration",
        "stakeCredentialDeregistration",
    }
)

_original_parse_Block = rh.parse_Block


def _enriched_parse_Block(resp: dict[str, Any]) -> Block:
    """Wrap parse_Block to add missing deposit fields to stake credential certificates."""
    block = _original_parse_Block(resp)
    transactions = getattr(block, "transactions", None)
    if not transactions:
        return block
    for tx in transactions:
        for cert in filter(
            lambda c: c["type"] in _CERT_TYPES_NEEDING_DEPOSIT and "deposit" not in c,
            tx.get("certificates", ()),
        ):
            cert["deposit"] = STAKE_CREDENTIAL_DEPOSIT
    return block


rh.parse_Block = _enriched_parse_Block


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

    async def follow(
        self, intersection: Point, *, in_flight: int
    ) -> AsyncIterator[RollForward | RollBackward]:
        """Every chain-sync reply from ``intersection`` on, in order, for good.

        ``batched`` cannot serve the tip: it times out a reply after 30s when
        the tip produces a block every ~20s on average and sometimes minutes
        apart, it discards rollbacks, and it deduplicates by height, which
        keeps whichever of two competing blocks arrived first. This yields
        both directions exactly as the node sent them.

        ``in_flight`` requests are kept outstanding so catching up is not one
        round trip per block. At the tip the node holds them until it has
        something to say, so no reply ever times out here — a dead connection
        surfaces from the websocket's own keepalive instead. A new request is
        only sent once the previous reply is consumed, so a caller that stops
        iterating (under backpressure, say) stops the flow of blocks too.

        Assumes the cursor was just placed at ``intersection`` by
        ``find_intersection``, whose first reply is a rollback to it.

        A block built on a recently yielded point other than the previous one
        is a fork switch the node did not announce — dolos sends none when it
        switches its tip — so the rollback it implies is yielded before it.

        :raises ChainLinkError: when a block names as its ancestor no point
            yielded within the security parameter, or the node rolls back to
            the origin.
        """
        cursor = intersection
        # Oldest first: every point a fork switch could be built on.
        held: OrderedDict[str, Point] = OrderedDict({intersection.id: intersection})
        for _ in range(in_flight):
            await self.send()

        while True:
            direction, tip, received, _ = await self.receive()
            await self.send()

            if not isinstance(tip, Tip):
                raise ChainLinkError("the node reports its tip as the origin")

            if direction is Direction.backward:
                if not isinstance(received, Point):
                    raise ChainLinkError("the node rolled back to the origin")
                cursor = received
                _keep_through(held, received)
                yield RollBackward(point=received, tip=tip)
                continue

            if received.id == cursor.id:
                # dolos rolls forward to its tip block a second time when a
                # session first reaches it (at once, if it intersected there).
                # The same block again extends nothing and orphans nothing.
                continue
            if received.ancestor != cursor.id:
                forked_from = held.get(received.ancestor)
                if forked_from is None:
                    raise ChainLinkError(
                        f"block {received.slot}.{received.id} names ancestor "
                        f"{received.ancestor}, but the previous point is "
                        f"{cursor.slot}.{cursor.id}"
                    )
                logger.warning(
                    "Node switched to block %s.%s, built on %s.%s, without rolling "
                    "back from %s.%s; relaying the rollback it implies",
                    received.slot,
                    received.id,
                    forked_from.slot,
                    forked_from.id,
                    cursor.slot,
                    cursor.id,
                )
                _keep_through(held, forked_from)
                cursor = forked_from
                yield RollBackward(point=forked_from, tip=tip)

            cursor = Point(slot=received.slot, id=received.id)
            held[cursor.id] = cursor
            if len(held) > SECURITY_PARAMETER:
                held.popitem(last=False)
            yield RollForward(block=received, tip=tip)
