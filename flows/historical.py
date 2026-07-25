import asyncio
import os
import time
from multiprocessing import cpu_count
from typing import Any

import ogmios.model.model_map as mm
from ogmios import Block
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect.task_runners import ProcessPoolTaskRunner

from client import HecateClient
from config.settings import batch_settings, redis_settings
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES, FIRST_SHELLEY_EPOCH
from epoch_cache import extend_cache, load_cache
from epoch_derivation import regenerate_range
from flows.stream_cleanup import cleanup_streams_loop
from models import BlockHeight, EpochData, EpochNumber
from network import NetworkManager
from sinks.metrics import heartbeat
from sinks.redis import HistoricalRedisSink

# Optional kupo endpoint to accelerate on-demand derivation of epochs missing
# from the committed CSVs; without it the derivation falls back to a chain-sync
# walk. On-demand derivation is a rare self-heal path — the committed data is
# normally kept current by the periodic flow.
KUPO_URL = os.getenv("KUPO_URL")


def fast_block_init(self: Block, blocktype: mm.Types, **kwargs: Any) -> None:
    """
    Fast initialization for Block objects that bypasses Pydantic validation.

    This optimized initialization directly assigns attributes from kwargs
    without constructing or validating Pydantic models. It's designed for
    processing historical blocks where validation is redundant.
    """
    self.blocktype = blocktype
    # Directly assign all attributes without creating _schematype
    for key, value in kwargs.items():
        setattr(self, key, value)
    # Set a dummy _schematype attribute to avoid attribute errors
    self._schematype = None


# Apply performance optimization for Block initialization
Block.__init__ = fast_block_init


@task(
    retries=3,
    retry_delay_seconds=10,
    cache_policy=NO_CACHE,
    task_run_name="sync_epoch_{epoch}",
)
async def sync_epoch(
    epoch: EpochNumber,
    endpoint: str,
    batch_size: int = 1000,
    previous_boundary: EpochData | None = None,
    block_count: int | None = None,
) -> EpochNumber:
    """
    Fetch all blocks for an epoch and XADD them directly to the per-epoch
    Redis stream (``epoch:{N}``). Ordering across epochs is guaranteed by
    design, as consumers read epoch streams in ascending order.

    ``previous_boundary`` and ``block_count`` carry the epoch's boundary data
    explicitly so worker processes need not read it from the committed CSVs —
    this is what lets on-demand-derived epochs be streamed.
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug("▶️  Starting sync for epoch %s on endpoint %s", epoch, endpoint)

    async with (
        HistoricalRedisSink() as sink,
        HecateClient(endpoint_url=endpoint) as client,
    ):
        # Wipe any stale buffer/resume state so retries always start clean.
        await sink.reset_epoch_state(epoch)

        last_height = await _stream_and_batch_blocks(
            client, sink, epoch, batch_size, logger, previous_boundary, block_count
        )

        if last_height is None:
            raise ValueError("No blocks fetched for epoch %s.", epoch)

    epoch_end = time.perf_counter()
    logger.info("✅ Epoch %s fetched in %.2fs", epoch, epoch_end - epoch_start)
    return epoch


def _should_skip(block: Block, resume_height: BlockHeight | None) -> bool:
    """Check if a given block should be skipped based on resume height, if any."""
    return resume_height is not None and block.height <= resume_height


async def _stream_and_batch_blocks(
    client: HecateClient,
    sink: HistoricalRedisSink,
    epoch: EpochNumber,
    batch_size: int,
    run_logger: Any,
    previous_boundary: EpochData | None = None,
    block_count: int | None = None,
) -> int | None:
    """
    Stream blocks from the client and process them in optimized batches.

    This function handles the core logic of streaming blocks from the Ogmios client,
    batching them for efficient processing and sending them to the sink. It supports
    resuming from a previous checkpoint if the epoch was partially processed.

    Args:
        client: The Hecate client for fetching blocks
        sink: Sink for storing processed blocks
        epoch: The epoch number to process
        batch_size: Batch size to send blocks in
        run_logger: Logger instance for this run

    Returns:
        The height of the last processed block, or None if no blocks were processed
    """
    batch: list[Block] = []
    last_height: int | None = None
    resume_height = await sink.get_epoch_resume_height(epoch)

    async for blocks in client.epoch_blocks(
        epoch, previous_boundary=previous_boundary, block_count=block_count
    ):
        for block in blocks:
            if _should_skip(block, resume_height):
                continue
            batch.append(block)
            last_height = block.height

            if len(batch) < batch_size:
                continue

            await sink.send_batch(batch, epoch=epoch)
            run_logger.debug("Sent batch of %d blocks for epoch %s", len(batch), epoch)
            batch.clear()

    # Send any remaining blocks
    if batch:
        await sink.send_batch(batch, epoch=epoch)
        run_logger.debug(
            "Sent final batch of %d blocks for epoch %s", len(batch), epoch
        )
        last_height = batch[-1].height
        batch.clear()

    return last_height


async def process_batch(
    total_epochs: int,
    max_concurrent_epochs: int,
    batch_start_index: int,
    epochs: list[EpochNumber],
    batch_size: int,
    network_manager: NetworkManager,
    boundaries: dict[EpochNumber, EpochData],
    counts: dict[EpochNumber, int],
) -> None:
    """
    Process a batch of epochs concurrently, then mark them complete in order.

    Phase 1 (concurrent): Each epoch task fetches blocks from Ogmios and
    XADDs them directly to their per-epoch Redis stream.
    Phase 2 (sequential): Mark each epoch complete in ascending order,
    advancing ``last_synced_epoch`` atomically.
    """
    logger = get_run_logger()
    batch_start_time = time.perf_counter()
    batch_epochs = epochs[batch_start_index : batch_start_index + max_concurrent_epochs]
    batch_number = (batch_start_index // max_concurrent_epochs) + 1
    total_batches = (total_epochs + max_concurrent_epochs - 1) // max_concurrent_epochs

    logger.info(
        "🟢 Starting batch %d/%d: epochs %d to %d",
        batch_number,
        total_batches,
        batch_epochs[0],
        batch_epochs[-1],
    )

    # Check backpressure before launching workers
    async with HistoricalRedisSink() as sink:
        await sink.wait_for_backpressure()
        assert sink.metrics
        await sink.metrics.note_workers_busy(
            active=len(batch_epochs),
            maximum=max_concurrent_epochs,
        )

    # Phase 1 — concurrent fetch + direct XADD to per-epoch streams
    batch_endpoints = [network_manager.get_connection() for _ in batch_epochs]

    futures = sync_epoch.map(
        epoch=batch_epochs,
        endpoint=batch_endpoints,
        batch_size=batch_size,
        previous_boundary=[boundaries[EpochNumber(e - 1)] for e in batch_epochs],
        block_count=[counts[e] for e in batch_epochs],
    )
    # `wait` is sync-blocking; off-loop it so the heartbeat task keeps firing.
    await asyncio.to_thread(wait, futures)

    # Phase 2 — mark epochs complete in order
    async with HistoricalRedisSink() as sink:
        for epoch in batch_epochs:
            last_height = await sink.get_epoch_resume_height(epoch)
            if last_height is None:
                logger.warning("Epoch %d has no data, skipping", epoch)
                continue

            await sink.mark_epoch_complete(epoch, last_height)

        assert sink.metrics
        await sink.metrics.note_workers_idle()

    batch_end_time = time.perf_counter()
    logger.info(
        "✅ Completed batch %d/%d in %.2fs",
        batch_number,
        total_batches,
        batch_end_time - batch_start_time,
    )


async def _resolve_target_and_boundaries(
    start_epoch: EpochNumber,
    end_epoch: EpochNumber | None,
    endpoint: str,
    logger: Any,
) -> tuple[EpochNumber, dict[EpochNumber, EpochData], dict[EpochNumber, int]]:
    """Determine the seed target from the live chain and assemble boundary data.

    The target is the last finalized epoch (the current open epoch minus one),
    read live from Ogmios rather than a checkpoint file — so a stale checkout can
    no longer silently clamp the seed range. Epochs beyond the frozen bootstrap
    CSVs and the local cache are derived from the chain on demand (kupo-accelerated
    when KUPO_URL is set, otherwise a pure chain-sync walk) and cached for reuse.

    Returns the target epoch plus boundary rows and block counts keyed by epoch,
    covering ``[start_epoch - 1, target]``.
    """
    boundaries: dict[EpochNumber, EpochData] = dict(EPOCH_BOUNDARIES)
    counts: dict[EpochNumber, int] = {
        EpochNumber(e): c for e, c in BLOCKS_IN_EPOCH.items()
    }
    # Layer the local cache of previously derived epochs over the frozen CSVs.
    cached_boundaries, cached_counts = load_cache()
    boundaries.update(cached_boundaries)
    counts.update(cached_counts)

    async with HecateClient(endpoint_url=endpoint) as client:
        current, _ = await client.epoch.execute()
        last_finalized = EpochNumber(current - 1)

        if end_epoch is not None and end_epoch > last_finalized:
            logger.warning(
                "end_epoch %d exceeds last finalized epoch %d — clamping to %d",
                end_epoch,
                last_finalized,
                last_finalized,
            )
        target = (
            last_finalized
            if end_epoch is None
            else EpochNumber(min(end_epoch, last_finalized))
        )

        highest_cached = max(boundaries)
        if target > highest_cached:
            logger.info(
                "Deriving epochs %d..%d absent from the committed CSVs",
                highest_cached + 1,
                target,
            )
            new_rows, new_counts = await regenerate_range(
                client,
                EpochNumber(highest_cached + 1),
                target,
                boundaries[highest_cached],
                kupo_url=KUPO_URL,
            )
            boundaries.update(new_rows)
            counts.update(new_counts)
            # Persist the newly derived tail so the next run reuses it.
            extend_cache(new_rows, new_counts)

    return target, boundaries, counts


@flow(
    name="Historical Sync",
    task_runner=ProcessPoolTaskRunner(max_workers=cpu_count()),  # type: ignore[arg-type]
)
async def historical_sync_flow(
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
    end_epoch: EpochNumber | None = None,
    batch_size: int | None = None,
    concurrent_epochs: int | None = None,
) -> None:
    """
    Retrieves and relays data across a range of epochs against the system checkpoint.
    This flow resumes from the last synced epoch if applicable,
    or starts from the specified starting epoch.
    The synchronization tasks are processed concurrently for improved performance.

    This asynchronous flow uses a Dask-based task runner to handle workloads and ensures
    data is passed along efficiently using defined batch sizes. The execution time is logged
    to monitor the performance of the process.

    :param start_epoch: The starting epoch for synchronization. Defaults to FIRST_SHELLEY_EPOCH.
    :type start_epoch: EpochNumber
    :param end_epoch: Optional upper-bound epoch (inclusive). When provided, sync stops
     at this epoch instead of the last finalized epoch. Clamped to the last finalized
     epoch (current open epoch − 1) if it exceeds it.
    :type end_epoch: EpochNumber | None
    :param batch_size: The number of blocks processed per batch for synchronization.
     Defaults to BASE_BATCH_SIZE from settings (typically 1000 in production).
    :type batch_size: int | None
    :param concurrent_epochs: The number of epochs to process concurrently.
     Defaults to DASK_N_WORKERS (6) from settings if not provided.
    :type concurrent_epochs: int | None
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()

    batch_size = batch_size or batch_settings.batch_size
    concurrent_epochs = concurrent_epochs or cpu_count()

    network_manager = NetworkManager()

    async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
        last = await sink.get_last_synced_epoch()
        sink_prefix = sink.prefix

    if last > start_epoch:
        logger.info(
            "🔄 Resuming after last synced epoch %d instead of %d", last, start_epoch
        )
        start_epoch = EpochNumber(last + 1)

    # Purge orphaned epoch streams below start_epoch from prior overlapping runs.
    # Without this, streams that no consumer will ever read (0 consumer groups)
    # block backpressure indefinitely.
    async with HistoricalRedisSink() as sink:
        await sink.purge_stale_streams(start_epoch)

    target, boundaries, counts = await _resolve_target_and_boundaries(
        start_epoch, end_epoch, network_manager.get_connection(), logger
    )

    epochs = [EpochNumber(e) for e in range(start_epoch, target + 1)]
    total_epochs = len(epochs)

    if not epochs:
        logger.info("No epochs to process")
        return

    logger.info(
        "Processing %d epochs with %d concurrent workers",
        total_epochs,
        concurrent_epochs,
    )

    # Run stream cleanup as a background asyncio task (not a Prefect task),
    # so it doesn't block the flow from completing when consumers are slow.
    cleanup = asyncio.create_task(
        cleanup_streams_loop(target_epoch=target, logger=logger)  # type: ignore[arg-type]
    )

    try:
        async with heartbeat(redis_settings.url, sink_prefix):
            for batch_start_index in range(0, total_epochs, concurrent_epochs):
                await process_batch(
                    total_epochs,
                    concurrent_epochs,
                    batch_start_index,
                    epochs,
                    batch_size,
                    network_manager,
                    boundaries,
                    counts,
                )
    finally:
        cleanup.cancel()

    flow_end = time.perf_counter()
    elapsed_time = flow_end - flow_start
    logger.info("🏁 Historical sync complete in %.2fs", elapsed_time)
