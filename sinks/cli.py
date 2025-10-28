from datetime import datetime
from typing import Any

from ogmios import Block
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from rich.table import Table

from sinks.base import DataSink


class CLISink(DataSink):
    """A pretty CLI data sink using rich"""

    def __init__(self, max_history: int = 5):
        """Initialize console and history tracking"""
        self.console = Console()
        self.max_history = max_history
        self.last_blocks: list[Block] = []
        self.stats: dict[str, Any] = {
            "blocks_processed": 0,
            "batches_processed": 0,
            "last_block_hash": "",
            "last_block_slot": 0,
            "start_time": datetime.now(),
        }

    async def send_block(self, block: Block) -> None:
        """Print a single block to the console"""
        self._update_stats(block)
        self._store_in_history(block)
        block_data = await self._prepare_block(block)

        self.console.print(
            Panel.fit(
                Pretty(block_data),
                title=f"[bold green]Block {block_data['hash'][:8]}...[/]",
                subtitle=f"Slot: {block_data['slot']}",
            )
        )

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Print a summary of the batch to the console"""
        if not blocks:
            return

        self.stats["batches_processed"] += 1
        self.stats["blocks_processed"] += len(blocks)

        # Update with last block
        self._update_stats(blocks[-1])

        # Store a few blocks in history
        for block in blocks[-self.max_history :]:
            self._store_in_history(block)

        # Create batch summary table
        table = Table(title=f"[bold]Processed Batch of {len(blocks)} Blocks[/]")
        table.add_column("First Block", style="cyan")
        table.add_column("Last Block", style="green")
        table.add_column("Slot Range", style="magenta")

        first_block, last_block = blocks[0], blocks[-1]
        table.add_row(
            first_block.id[:8] + "...",
            last_block.id[:8] + "...",
            f"{first_block.slot} → {last_block.slot}",
        )

        self.console.print(table)

    async def get_status(self) -> dict[str, Any]:
        uptime = datetime.now() - self.stats["start_time"]

        self.console.print("[bold blue]Hecate Status[/bold blue]")
        self.console.print(
            f"Uptime: {uptime}\n"
            f"Blocks processed: {self.stats['blocks_processed']}\n"
            f"Batches processed: {self.stats['batches_processed']}\n"
            f"Last block: {self.stats['last_block_hash'][:8]}... "
            f"(slot {self.stats['last_block_slot']})"
        )

        return self.stats

    async def close(self) -> None:
        """Close the sink - print goodbye message"""
        self.console.print("[bold green]Hecate CLI sink closed[/]")

    def _update_stats(self, block: Block) -> None:
        """Update internal statistics"""
        self.stats["last_block_hash"] = block.id
        self.stats["last_block_slot"] = block.slot
        self.stats["blocks_processed"] += 1

    def _store_in_history(self, block: Block) -> None:
        """Store block in history, maintaining max size"""
        self.last_blocks.append(block)
        if len(self.last_blocks) > self.max_history:
            self.last_blocks.pop(0)

    @classmethod
    async def _prepare_block(cls, block: Block) -> dict[str, Any]:
        """Prepare a block for sending to the sink"""
        return {
            "hash": block.id,
            "slot": block.slot,
            "height": block.height,
            "era": block.era,
            "issuer": block.issuer["verificationKey"],
            "tx_count": len(block.transactions),
        }
