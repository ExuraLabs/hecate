from collections import deque
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, TypeVar, runtime_checkable

from ogmios import Block

from models import BlockHeight, EpochNumber, Slot


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

    async def get_last_synced_epoch(self) -> EpochNumber:
        """Highest epoch N with every epoch through N completed."""
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

    async def purge_stale_streams(self, up_to_epoch: EpochNumber) -> int:
        """Drop orphaned published data below ``up_to_epoch``."""
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


T = TypeVar("T", bound=DataSink)


class BufferedSink:
    """
    Wrapper for any DataSink that buffers blocks until they reach
    the required confirmation depth before sending them downstream. Useful for live tracking.
    """

    def __init__(self, sink: T, confirmation_depth: int = 5):
        """
        Initialize with any DataSink implementation and confirmation depth.

        Args:
            sink: Any DataSink implementation
            confirmation_depth: Number of blocks to wait before confirming
        """
        self.sink = sink
        self.confirmation_depth = confirmation_depth
        self.buffer: deque[Block] = deque()
        self.last_confirmed_slot = 0

    async def send_block(self, block: Block) -> None:
        self.buffer.append(block)
        await self._flush_confirmed()

    async def send_batch(self, blocks: list[Block]) -> None:
        """
        Buffer all blocks in the batch and flush any confirmed blocks.
        """
        for block in blocks:
            self.buffer.append(block)

        await self._flush_confirmed()

    async def get_status(self) -> dict[str, Any]:
        """
        Get underlying sink status along with buffer information.
        """
        sink_status = await self.sink.get_status()
        buffer_status = {
            "buffered_blocks": len(self.buffer),
            "confirmation_depth": self.confirmation_depth,
            "last_confirmed_slot": self.last_confirmed_slot,
        }

        return {**sink_status, "buffer": buffer_status}

    async def close(self) -> None:
        await self.sink.close()

    async def _flush_confirmed(self) -> None:
        """
        Send all blocks that have reached confirmation depth.
        """
        if len(self.buffer) <= self.confirmation_depth:
            return  # Not enough blocks to confirm any

        to_flush = len(self.buffer) - self.confirmation_depth

        if to_flush <= 0:
            return

        # Send the confirmed blocks downstream
        confirmed_blocks = []
        for _ in range(to_flush):
            block = self.buffer.popleft()
            confirmed_blocks.append(block)
            # Track last confirmed slot
            slot = block.slot
            if slot > self.last_confirmed_slot:
                self.last_confirmed_slot = slot

        if confirmed_blocks:
            await self.sink.send_batch(confirmed_blocks)

    async def rollback_to_slot(self, rollback_slot: Slot) -> None:
        """
        Handle a rollback by removing affected blocks from the buffer.
        """
        while self.buffer and self.buffer[0].slot != rollback_slot:
            self.buffer.popleft()
