from collections import deque
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ogmios import Block, Point

from errors import ChainLinkError
from models import BlockHeight, EpochNumber


def prepare_block(block: Block) -> dict[str, Any]:
    """Serialize a block to the wire format every sink relays.

    Sends the whole block minus the fields no downstream consumer needs:
    the pydantic schema handle, the issuer, and per-transaction datums,
    scripts and redeemers. ``hash`` and ``slot`` are emitted first so the
    two fields consumers key on stay at the head of the payload.

    Module-level rather than a sink method: the format is a property of
    Hecate's output contract, not of any one sink.
    """
    block_data: dict[str, Any] = {"slot": -1, "hash": block.id}

    filtered_block_fields = ("_schematype", "issuer", "id")
    block_data |= {
        field: value
        for field, value in block.__dict__.items()
        if field not in filtered_block_fields
        and field != "transactions"  # Handled next
    }

    filtered_tx_fields = ("datums", "scripts", "redeemers")
    block_data["transactions"] = [
        {field: value for field, value in tx.items() if field not in filtered_tx_fields}
        for tx in block.transactions
    ]

    return block_data


class BlockRelay(Protocol):
    """The narrow surface ``backfill()`` needs from a sink.

    This is the whole contract for relaying blocks somewhere. A third
    party integrating Hecate implements ``send_batch`` plus the async
    context manager and is done — everything else in this module is
    optional extra credit.
    """

    async def __aenter__(self) -> "BlockRelay": ...

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any, /) -> None: ...

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Send a batch of blocks to the sink."""
        ...


@runtime_checkable
class EpochCoordinator(BlockRelay, Protocol):
    """The ordering/durability surface, layered on top of a relay.

    A sink that implements this additionally owns *where the backfill is*:
    resume positions, ordered epoch completion, consumer backpressure and
    the lifecycle of already-published data. ``backfill()`` detects it at
    runtime (``isinstance``) and stays fully functional without it — a
    coordinator-less sink simply gets every block of every requested
    epoch, with no resumability and no flow control.
    """

    async def ensure_ordering_base(self, base: EpochNumber) -> EpochNumber:
        """Seed the ordering base if untouched, and report the one in force.

        The base is where ordered completion starts counting from. An
        existing one is returned unchanged rather than overwritten: only the
        caller knows whether it matches the window about to be relayed, and
        a mismatch is not something a sink may paper over.
        """
        ...

    async def reset_ordering_base(self, base: EpochNumber) -> None:
        """Move the base, dropping any claim that earlier epochs were sent.

        A deliberate act of scope-narrowing, not a repair — call it only when
        the epochs being written off are genuinely out of scope.
        """
        ...

    async def get_last_synced_epoch(self) -> EpochNumber | None:
        """Highest epoch N with every epoch through N completed.

        None when nothing has been delivered yet. This is the value
        consumers bound their reads by.
        """
        ...

    async def get_epoch_resume_height(self, epoch: EpochNumber) -> BlockHeight | None:
        """Height already relayed for an in-progress epoch, if any."""
        ...

    async def reset_epoch_state(self, epoch: EpochNumber) -> None:
        """Discard partial state for an epoch, so a retry starts clean."""
        ...

    async def mark_epoch_complete(
        self, epoch: EpochNumber, last_height: BlockHeight
    ) -> EpochNumber:
        """Publish an epoch as complete; returns the new last-synced epoch."""
        ...

    async def purge_stale_streams(
        self, up_to_epoch: EpochNumber, *, purge_orphans: bool = False
    ) -> int:
        """Reclaim published data below ``up_to_epoch`` that is finished with.

        Must refuse — not delete — anything a consumer could still be owed.
        ``purge_orphans`` opts into dropping data no consumer has registered
        for, which is otherwise indistinguishable from data whose consumer has
        not started yet.
        """
        ...

    async def wait_for_backpressure(self) -> None:
        """Block until consumers can accept more data."""
        ...

    async def note_batch_started(self, *, active: int, maximum: int) -> None:
        """Record that a concurrent batch of epochs is in flight."""
        ...

    async def note_batch_finished(self) -> None:
        """Record that no epoch workers are running."""
        ...

    def run_bookkeeping(
        self, *, target_epoch: EpochNumber
    ) -> AbstractAsyncContextManager[None]:
        """Run this sink's own background upkeep for the duration of the body.

        Whatever the sink needs kept alive alongside a backfill —
        liveness signalling, reclaiming already-consumed data — without
        the backfill needing to know what any of it is.
        """
        ...


class DataSink(BlockRelay, Protocol):
    """A fuller sink: batches, single blocks, status and teardown."""

    async def send_block(self, block: Block) -> None:
        """Send a block to the sink"""
        ...

    async def get_status(self) -> dict[str, Any]:
        """Get sink status information"""
        ...

    async def close(self) -> None:
        """Close sink connections, if any"""
        ...


class RollbackRelay(Protocol):
    """A relay that can be told the chain rolled back past what it holds.

    What ``BufferedSink`` needs downstream: blocks in chain order, and word
    of any rollback deeper than the blocks it is still holding back.
    """

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Send blocks that each extend the one before, oldest first."""
        ...

    async def send_rollback(self, point: Point, **kwargs: Any) -> None:
        """Record that everything relayed after ``point`` is no longer on chain."""
        ...


@dataclass(frozen=True, slots=True)
class _Held:
    """A block not yet relayed, with the context it arrived with."""

    block: Block
    context: dict[str, Any]


class BufferedSink:
    """Holds the newest ``depth`` blocks back, so shallow rollbacks never escape.

    Most rollbacks at the tip are a block or two deep. A block is relayed
    downstream only once ``depth`` blocks have been built on top of it, so a
    rollback that lands among the blocks still held is absorbed here: the
    blocks after the rollback point are dropped and downstream never learns
    they existed. Only a rollback below everything held reaches downstream,
    as ``send_rollback``.

    Every block is checked to extend the one before it — the newest held
    block, else the last point relayed (``base``) — and a rollback must name a
    point on that chain. Anything else raises ``ChainLinkError`` before the
    buffer changes, since relaying past a break would hand downstream a chain
    that does not exist.

    ``base`` starts as the point the chain is being followed from: the first
    block must name it as its ancestor.
    """

    def __init__(self, sink: RollbackRelay, *, base: Point, depth: int = 2):
        if depth < 0:
            raise ValueError(f"depth must be at least 0, got {depth}")
        self.sink = sink
        self.depth = depth
        #: The newest point relayed downstream, or the starting point.
        self.base: Point = base
        self.held: deque[_Held] = deque()

    @property
    def head(self) -> Point:
        """The newest point this buffer knows of, relayed or not."""
        if self.held:
            newest = self.held[-1].block
            return Point(slot=newest.slot, id=newest.id)
        return self.base

    async def send_block(self, block: Block, **kwargs: Any) -> None:
        await self.send_batch([block], **kwargs)

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Hold ``blocks`` and relay whichever now have ``depth`` successors.

        ``kwargs`` travel with each block and reach downstream with it, so a
        block relayed later still carries the context it arrived with.
        """
        head = self.head
        for block in blocks:
            if block.ancestor != head.id:
                raise ChainLinkError(
                    f"block {block.slot}.{block.id} names ancestor "
                    f"{block.ancestor}, but the chain so far ends at "
                    f"{head.slot}.{head.id}"
                )
            head = Point(slot=block.slot, id=block.id)

        self.held.extend(_Held(block, dict(kwargs)) for block in blocks)
        await self._release()

    async def rollback_to(self, point: Point, **kwargs: Any) -> None:
        """Discard everything after ``point``, telling downstream only if it must.

        A point among the held blocks, or the last point relayed, is absorbed
        here. A point below that empties the buffer and is passed downstream
        with ``kwargs``.
        """
        keep = self._held_through(point)
        if keep is not None:
            while len(self.held) > keep:
                self.held.pop()
            return

        if point.slot >= self.base.slot:
            raise ChainLinkError(
                f"rollback to {point.slot}.{point.id}, which is not on the "
                f"chain held here (relayed through {self.base.slot}."
                f"{self.base.id}, holding {len(self.held)} more)"
            )

        self.held.clear()
        await self.sink.send_rollback(point, **kwargs)
        self.base = point

    def _held_through(self, point: Point) -> int | None:
        """How many held blocks survive a rollback to ``point``.

        None when ``point`` is neither a held block nor the base. A point
        whose slot matches but whose hash does not is someone else's block at
        that slot, never ours.
        """
        for index in range(len(self.held) - 1, -1, -1):
            block = self.held[index].block
            if block.slot == point.slot and block.id == point.id:
                return index + 1
        if self.base.slot == point.slot and self.base.id == point.id:
            return 0
        return None

    async def _release(self) -> None:
        """Relay every block that now has ``depth`` blocks on top of it.

        Consecutive blocks that arrived with the same context go downstream
        in one batch; the base only advances once downstream has taken them.
        """
        while len(self.held) > self.depth:
            first = self.held[0]
            run = [first.block]
            while (
                len(self.held) - len(run) > self.depth
                and self.held[len(run)].context == first.context
            ):
                run.append(self.held[len(run)].block)

            await self.sink.send_batch(run, **first.context)
            for _ in run:
                self.held.popleft()
            last = run[-1]
            self.base = Point(slot=last.slot, id=last.id)
