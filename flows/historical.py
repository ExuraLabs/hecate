import time
from typing import Any
from multiprocessing import cpu_count

from ogmios import Block
import ogmios.model.model_map as mm
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect.task_runners import ProcessPoolTaskRunner

from client import HecateClient
from config.settings import batch_settings
from constants import FIRST_SHELLEY_EPOCH
from flows import get_system_checkpoint
from models import BlockHeight, EpochNumber
from network import NetworkManager
from sinks.redis import HistoricalRedisSink
from sinks.stream_cleanup import cleanup_redis_streams_task


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
) -> EpochNumber:
    """
    Synchronize a specific epoch by fetching blocks in batches.
    Uses optimized connection pooling and memory management for maximum performance.
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug("▶️  Starting sync for epoch %s on endpoint %s", epoch, endpoint)

    async with (
        HistoricalRedisSink() as sink,
        HecateClient(endpoint_url=endpoint) as client,
    ):
        last_height = await _stream_and_batch_blocks(
            client, sink, epoch, batch_size, logger
        )

        if last_height is None:
            logger.warning(
                "No blocks processed for epoch %s, cannot mark complete.", epoch
            )
            return epoch

        await sink.mark_epoch_complete(epoch, BlockHeight(last_height))
        logger.info("✅ Epoch %s completed", epoch)

    epoch_end = time.perf_counter()
    logger.info("✅ Epoch %s sync complete in %.2fs", epoch, epoch_end - epoch_start)
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

    async for blocks in client.epoch_blocks(epoch):
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
) -> None:
    """
    Process a batch of epochs concurrently using sync_epoch tasks.

    This function takes a slice of epochs and processes them concurrently
    using Prefect's task mapping functionality with Dask. It provides progress
    logging and timing information for each batch.

    Args:
        total_epochs: Total number of epochs to process across all batches
        max_concurrent_epochs: Maximum number of epochs to process simultaneously
        batch_start_index: Starting index in the epoch list for this batch
        epochs: Complete list of epochs to process
        batch_size: Number of blocks to process per epoch batch
        network_manager: NetworkManager instance for managing connections
    """
    logger = get_run_logger()
    batch_start_time = time.perf_counter()
    batch_epochs = epochs[batch_start_index : batch_start_index + max_concurrent_epochs]
    batch_number = (batch_start_index // max_concurrent_epochs) + 1
    total_batches = (total_epochs + max_concurrent_epochs - 1) // max_concurrent_epochs

    logger.info(
        "🔄 Starting batch %d/%d: epochs %d to %d",
        batch_number,
        total_batches,
        batch_epochs[0],
        batch_epochs[-1],
    )

    batch_endpoints = [network_manager.get_connection() for _ in batch_epochs]

    futures = sync_epoch.map(
        epoch=batch_epochs, endpoint=batch_endpoints, batch_size=batch_size
    )
    wait(futures)

    batch_end_time = time.perf_counter()
    logger.info(
        "✅ Completed batch %d/%d in %.2fs",
        batch_number,
        total_batches,
        batch_end_time - batch_start_time,
    )


@flow(
    name="Historical Sync",
    task_runner=ProcessPoolTaskRunner(max_workers=cpu_count()),  # type: ignore[arg-type]
)
async def historical_sync_flow(
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
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
    :param batch_size: The number of blocks processed per batch for synchronization.
     Defaults to BASE_BATCH_SIZE from settings (typically 1000 in production).
    :type batch_size: int | None
    :param concurrent_epochs: The number of epochs to process concurrently.
     Defaults to DASK_N_WORKERS (6) from settings if not provided.
    :type concurrent_epochs: int | None
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()

    cleanup_redis_streams_task.submit()

    batch_size = batch_size or batch_settings.batch_size
    concurrent_epochs = concurrent_epochs or cpu_count()

    network_manager = NetworkManager()

    async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
        last = await sink.get_last_synced_epoch()

    if last > start_epoch:
        logger.info(
            "🔄 Resuming after last synced epoch %d instead of %d", last, start_epoch
        )
        start_epoch = EpochNumber(last + 1)

    target = get_system_checkpoint()
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

    # Process epochs in batches
    for batch_start_index in range(0, total_epochs, concurrent_epochs):
        await process_batch(
            total_epochs,
            concurrent_epochs,
            batch_start_index,
            epochs,
            batch_size,
            network_manager,
        )

    flow_end = time.perf_counter()
    elapsed_time = flow_end - flow_start
    logger.info("🏁 Historical sync complete in %.2fs", elapsed_time)
