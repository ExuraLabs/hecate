import time
import asyncio
from asyncio.log import logger
from typing import Any

import ogmios.model.model_map as mm
from ogmios import Block
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect_dask import DaskTaskRunner  # type: ignore[attr-defined]

from client import HecateClient
from config.settings import get_batch_settings, get_dask_settings
from constants import FIRST_SHELLEY_EPOCH
from flows import get_system_checkpoint
from models import BlockHeight, EpochNumber
from monitoring.metrics_agent import collect_and_publish_metrics
from network.endpoint_scout import EndpointScout
from sinks.redis import HistoricalRedisSink


def fast_block_init(self: Block, blocktype: mm.Types, **kwargs: Any) -> None:
    """
    Fast initialization for Block objects that bypasses Pydantic validation.

    This optimized initialization directly assigns attributes from kwargs
    without constructing or validating Pydantic models. It's designed for
    processing historical blocks where validation is redundant.

    Note:
        This method omits all validation checks present in the original
        implementation. Type mismatches or missing fields won't raise
        errors, which is acceptable for historical data but potentially
        dangerous for real-time blocks.

    Performance:
        When processing hundreds of thousands of blocks, this can significantly
        reduce CPU without affecting correctness.
    """
    self.blocktype = blocktype
    # Directly assign all attributes without creating _schematype
    for key, value in kwargs.items():
        setattr(self, key, value)
    # Set a dummy _schematype attribute to avoid attribute errors
    self._schematype = None


@task(
    retries=3,
    retry_delay_seconds=30,
    cache_policy=NO_CACHE,
    task_run_name="sync_epoch_{epoch}",
)
async def sync_epoch(
    epoch: EpochNumber,
    batch_size: int = 1000,
) -> EpochNumber:
    """
    Synchronize a specific epoch by fetching blocks of data in batches and 
    relaying them. This function handles connection management, error recovery,
    and proper cleanup for processing a single epoch.
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug("▶️  Starting sync for epoch %s", epoch)

    scout = EndpointScout()
    try:
        connection = await scout.get_best_connection()
        
        async with (HistoricalRedisSink() as sink, HecateClient(connection) as client):
            last_height = await _process_epoch_blocks(
                client, sink, epoch, batch_size, logger
            )

            if last_height is None:
                logger.warning("No blocks processed for epoch %s, cannot mark complete.", epoch)
                return epoch
                
            await sink.mark_epoch_complete(epoch, BlockHeight(last_height))
            logger.info("✅ Epoch %s completed", epoch)

        epoch_end = time.perf_counter()
        logger.info("✅ Epoch %s sync complete in %.2fs", epoch, epoch_end - epoch_start)
        return epoch

    except Exception as e:
        logger.error("Failed to sync epoch %s: %s", epoch, e)
        raise
    finally:
        await scout.close_all_connections()


async def _process_epoch_blocks(
    client: HecateClient,
    sink: HistoricalRedisSink,
    epoch: EpochNumber,
    initial_batch_size: int,
    run_logger: Any,
) -> int | None:
    """Process all blocks for an epoch, handling batching and memory management."""
    Block.__init__ = fast_block_init

    return await _stream_and_batch_blocks(
        client, sink, epoch, initial_batch_size, run_logger
    )


async def _stream_and_batch_blocks(
    client: HecateClient,
    sink: HistoricalRedisSink,
    epoch: EpochNumber,
    initial_batch_size: int,
    run_logger: Any,
) -> int | None:
    """
    Stream blocks from client and process them in adaptive batches.
    
    This function handles the core logic of streaming blocks from the Ogmios client,
    batching them for efficient processing, and sending them to the sink. It supports
    resuming from a previous checkpoint if the epoch was partially processed.
    
    Args:
        client: The Hecate client for fetching blocks
        sink: Sink for storing processed blocks  
        epoch: The epoch number to process
        initial_batch_size: Starting size for batches
        run_logger: Logger instance for this run
        
    Returns:
        The height of the last processed block, or None if no blocks were processed
    """
    try:
        resume_height = await sink.get_epoch_resume_height(epoch)
        
        batch: list[Block] = []
        blocks_processed = 0
        last_height: int | None = None
        current_batch_size = initial_batch_size

        async for blocks in client.epoch_blocks(epoch):
            for block in blocks:
                if _should_skip_block(block, resume_height):
                    continue
                    
                batch.append(block)
                last_height = block.height

                # Send batch when it reaches the target size
                if len(batch) >= current_batch_size:
                    try:
                        await sink.send_batch(batch, epoch=epoch)
                        run_logger.debug("Sent batch of %d blocks for epoch %s", len(batch), epoch)
                        blocks_processed += len(batch)
                        batch.clear()
                    except Exception as e:
                        run_logger.error("Failed to send batch for epoch %s: %s", epoch, e)
                        # Don't clear batch on failure, will retry
                        raise

        # Send any remaining blocks
        if batch:
            try:
                await sink.send_batch(batch, epoch=epoch)
                run_logger.debug("Sent batch of %d blocks for epoch %s", len(batch), epoch)
                blocks_processed += len(batch)
                if batch:
                    last_height = batch[-1].height
            except Exception as e:
                run_logger.error("Failed to send final batch for epoch %s: %s", epoch, e)
                raise

        return last_height
        
    except Exception as e:
        run_logger.error("Error streaming blocks for epoch %s: %s", epoch, e)
        raise


def _should_skip_block(block: Block, resume_height: BlockHeight | None) -> bool:
    """Check if a block should be skipped based on resume height."""
    return resume_height is not None and block.height <= resume_height


@flow(  # type: ignore[arg-type]
    name="Historical Sync",
    task_runner=DaskTaskRunner(  # type: ignore[arg-type]
        cluster_kwargs={
            "n_workers": get_dask_settings().n_workers,
            "threads_per_worker": 1,
            "memory_limit": get_dask_settings().worker_memory_limit,
        },
    ),
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
    :param batch_size: The number of records processed per batch for synchronization. Defaults to 100.
    :type batch_size: int
    :param concurrent_epochs: The number of epochs to process concurrently before waiting. Defaults to 6.
    :type concurrent_epochs: int
    :return: This flow does not return any value.
    :rtype: None
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()
    metrics_task: asyncio.Task[None] | None = None

    try:
        # Load settings from centralized config if not provided explicitly
        batch_settings = get_batch_settings()
        final_batch_size = batch_size or batch_settings.base_size
        final_concurrent_epochs = concurrent_epochs or get_dask_settings().n_workers

        # Initialize EndpointScout for connection management
        # scout = EndpointScout()

        # Start metrics collection in the background
        logger.info("Starting metrics collection task before historical sync...")
        metrics_task = asyncio.create_task(collect_and_publish_metrics())

        async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
            last = await sink.get_last_synced_epoch()

        if last > start_epoch:
            logger.info(
                "🔄 Resuming after last synced epoch %d instead of %d",
                last, start_epoch
            )
            start_epoch = EpochNumber(last + 1)

        target = get_system_checkpoint()
        epochs = [EpochNumber(e) for e in range(start_epoch, target + 1)]
        total_epochs = len(epochs)

        logger.info(
            "Processing %d epochs in batches of %d",
            total_epochs, final_concurrent_epochs
        )

        # Process epochs in batches
        for batch_start_index in range(0, total_epochs, final_concurrent_epochs):
            try:
                process_batch(
                    total_epochs,
                    final_concurrent_epochs,
                    batch_start_index,
                    epochs,
                    final_batch_size
                )
            except (ConnectionError, TimeoutError, RuntimeError, OSError, asyncio.TimeoutError) as e:
                logger.error(
                    "Failed to process batch starting at index %d: %s",
                    batch_start_index, e
                )
                # Continue with next batch rather than failing completely
                continue

        flow_end = time.perf_counter()
        logger.info("🏁 Historical sync complete in %.2fs", flow_end - flow_start)

    except (ConnectionError, TimeoutError, RuntimeError, OSError, asyncio.TimeoutError, ValueError, TypeError) as e:
        logger.error("Critical error in historical sync flow: %s", e)
        raise
    finally:
        # Ensure metrics task is properly cleaned up
        if metrics_task and not metrics_task.done():
            logger.info("Shutting down metrics collection task...")
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                logger.debug("Metrics task cancelled successfully")
            except (ConnectionError, TimeoutError, RuntimeError, OSError) as e:
                logger.warning("Error during metrics task cleanup: %s", e)

    
def process_batch(
    total_epochs: int,
    max_concurrent_epochs: int,
    batch_start_index: int,
    epochs: list[EpochNumber],
    batch_size: int,
    # scout: EndpointScout
) -> None:
    """
    Process a batch of epochs concurrently using sync_epoch tasks.
    
    This function takes a slice of epochs and processes them concurrently
    using Prefect's task mapping functionality. It provides progress
    logging and timing information for each batch.
    
    Args:
        total_epochs: Total number of epochs to process across all batches
        max_concurrent_epochs: Maximum number of epochs to process simultaneously
        batch_start_index: Starting index in the epochs list for this batch
        epochs: Complete list of epochs to process
        batch_size: Number of blocks to process per epoch batch
        
    Note:
        The scout parameter is commented out but would be used for connection management
    """
    batch_start_time = time.perf_counter()
    batch_epochs = epochs[batch_start_index : batch_start_index + max_concurrent_epochs]
    batch_number = (batch_start_index // max_concurrent_epochs) + 1
    total_batches = (
        total_epochs + max_concurrent_epochs - 1
    ) // max_concurrent_epochs

    logger.info(
        "🔄 Starting batch %d/%d: epochs %d to %d",
        batch_number, total_batches, batch_epochs[0], batch_epochs[-1]
    )

    futures = sync_epoch.map(
        epoch=batch_epochs,
        # scout=scout,
        batch_size=batch_size
    )
    wait(futures)

    batch_end_time = time.perf_counter()
    logger.info(
        "✅ Completed batch %d/%d in %.2fs",
        batch_number, total_batches, batch_end_time - batch_start_time
    )