from collections import deque
from typing import Any, Protocol, TypeVar

from ogmios import Block

from models import Slot


class DataSink(Protocol):
    """Protocol defining the interface for any data sink used by Hecate"""

    async def _prepare_block(self, block: Block) -> dict[str, Any]:
        """Prepare a block for sending to the sink"""
        ...

    async def send_block(self, block: Block) -> None:
        """Send a block to the sink"""
        ...

    async def send_batch(self, blocks: list[Block]) -> None:
        """Send a batch of blocks to the sink"""
        ...

    async def get_status(self) -> dict[str, Any]:
        """Get sink status information"""
        ...

    async def close(self) -> None:
        """Close sink connections, if any"""
        ...


T = TypeVar('T', bound=DataSink)


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
