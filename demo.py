import asyncio

from ogmios import Point
from rich.console import Console

from client import HecateClient
from constants import ERA_BOUNDARY
from sinks import CLISink, BufferedSink


async def run_demo() -> None:
    console = Console()
    console.print("[bold magenta]🔮 Hecate Client Demo 🔮[/]", justify="center")
    console.print()

    sink = CLISink()
    client = None

    try:
        console.print("[bold blue]Connecting to Ogmios...[/]")
        client = HecateClient()

        # 1. Get current tip
        console.print("[bold blue]🔎 Fetching current chain tip...[/]")
        tip, _ = await client.chain_tip.execute()
        console.print(
            f"🎯 Chain tip: [bold]Slot {tip.slot}[/] - Hash: {tip.id[:8]}...{tip.id[-8:]}"
        )
        console.print()

        # 2. Find intersection with Alonzo era start
        alonzo_slot, alonzo_hash = ERA_BOUNDARY["alonzo"]
        alonzo_point = Point(slot=alonzo_slot, id=alonzo_hash)

        console.print("[bold blue]🔙 Finding intersection with Alonzo era...[/]")
        intersection, _, _ = await client.find_intersection.execute(
            points=[alonzo_point]
        )

        console.print(
            f"Intersection found at: Slot {intersection.slot} - "
            f"Hash: {intersection.id[:8]}...{intersection.id[-8:]}"
        )

        # 3. Fetch a single block and send to sink
        console.print("[bold blue]1️⃣ Fetching a single block...[/]")
        await client.next_block.execute()  # Skip the first response
        direction, _, block, _ = await client.next_block.execute()

        # Send to sink
        await sink.send_block(block)

        # 4. Get a small batch of blocks
        console.print("[bold blue]📦 Fetching a small batch of blocks...[/]")
        batch = await client.next_block.batched(batch_size=3)

        # Send batch to sink
        await sink.send_batch(batch)

        # Wrap the sink in a BufferedSink to handle rollbacks
        buffered = BufferedSink(sink, confirmation_depth=3)
        console.print(
            f"[bold yellow]Test {buffered.confirmation_depth}-block buffer...[/]"
        )

        # Grab a few more blocks - these will be buffered
        batch = await client.next_block.batched(batch_size=3)
        await buffered.send_batch(batch)
        console.print(
            f"[bold blue]Buffered {len(batch)} blocks: {batch[0].slot} → {batch[-1].slot}[/]"
        )

        # 5. Simulate reorg - Next block's direction is "rollback"
        rollback_point = Point(slot=batch[0].slot, id=batch[0].id)
        console.print(
            f"[bold blue]🔁 Simulating a chain reorganization to slot {rollback_point.slot}...[/]"
        )
        intersection, _, _ = await client.find_intersection.execute(
            points=[rollback_point]
        )

        # Notify the sink about rollback
        console.print("[bold cyan]⏪ Rolling back buffered blocks...[/]")
        await buffered.rollback_to_slot(rollback_point.slot)

        # Get new blocks after rollback
        console.print("[bold blue]▶️ Resuming sync after rollback...[/]")
        new_batch = await client.next_block.batched(batch_size=2)
        await buffered.send_batch(new_batch)

        status = await buffered.get_status()
        console.print(
            "[bold yellow]📊 Last confirmed slot:[/] "
            f"[bold]{status['buffer']['last_confirmed_slot']}[/]"
        )
        console.print("[bold green]✅ Demo completed successfully! ✅[/]")

    except Exception as e:
        console.print(f"[bold red]Error:[/] {str(e)}")
        raise
    finally:
        if client:
            await client.close()
        await sink.close()


if __name__ == "__main__":
    asyncio.run(run_demo())
