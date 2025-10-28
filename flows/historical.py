import asyncio
import time
from typing import Any

import ogmios.model.model_map as mm
from ogmios import Block
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from client import HecateClient
from config.settings import get_batch_settings, get_concurrency_settings
from constants import FIRST_SHELLEY_EPOCH
from flows import get_system_checkpoint
from models import BlockHeight, EpochNumber
from monitoring.metrics_agent import collect_and_publish_metrics, MetricsAgent
from sinks.redis import HistoricalRedisSink


COMMON_ERRORS = (ConnectionError, TimeoutError, RuntimeError, OSError,
                 asyncio.TimeoutError)


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


def _should_skip_block(block: Block, resume_height: BlockHeight | None) -> bool:
    """Check if a block should be skipped based on resume height."""
    return resume_height is not None and block.height <= resume_height


@task(
    retries=3,
    retry_delay_seconds=15,
    cache_policy=NO_CACHE,
    task_run_name="sync_epoch_{epoch}",
)
async def sync_epoch(
    epoch: EpochNumber,
    batch_size: int = 1000,
) -> EpochNumber:
    """
    Synchronize a specific epoch by fetching blocks in batches.
    Uses optimized connection pooling and memory management for maximum performance.
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug("▶️  Starting sync for epoch %s", epoch)

    # Apply performance optimization for Block initialization
    Block.__init__ = fast_block_init
    
    # Use HecateClient with managed connections for efficiency
    async with (HistoricalRedisSink() as sink, HecateClient(use_managed_connection=True) as client):
        last_height = await _stream_and_batch_blocks(
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


async def _stream_and_batch_blocks(
    client: HecateClient,
    sink: HistoricalRedisSink,
    epoch: EpochNumber,
    initial_batch_size: int,
    run_logger: Any,
    ) -> int | None:
    """
    Stream blocks from client and process them in optimized batches.
    
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
                run_logger.debug("Sent final batch of %d blocks for epoch %s", len(batch), epoch)
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


@flow(name="Historical Sync")
async def historical_sync_flow(
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
    end_epoch: EpochNumber | None = None,
    batch_size: int | None = None,
) -> None:
    """
    Retrieves and relays data across a range of epochs with maximum efficiency.
    This flow resumes from the last synced epoch if applicable,
    or starts from the specified starting epoch.
    
    Uses Prefect's native concurrency with optimized task execution for performance
    comparable to Dask while maintaining all Prefect benefits.

    Args:
        start_epoch: Starting epoch for synchronization
        end_epoch: Ending epoch (if None, uses system checkpoint)
        batch_size: Blocks processed per batch (if None, uses settings)
        
    Returns:
        Dictionary containing sync results and metrics
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()

    metrics_agent = MetricsAgent.get_instance()

    try:
        batch_settings = get_batch_settings()
        concurrency_settings = get_concurrency_settings()
        final_batch_size = batch_size or batch_settings.base_size
        max_concurrent_epochs = concurrency_settings.max_workers

        logger.info("Collecting initial metrics before historical sync...")
        try:
            await collect_and_publish_metrics(metrics_agent)
        except COMMON_ERRORS as e:
            logger.warning("Failed to collect initial metrics: %s", e)

        async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
            last = await sink.get_last_synced_epoch()

        if last > start_epoch:
            logger.info("🔄 Resuming after last synced epoch %d instead of %d", last, start_epoch)
            start_epoch = EpochNumber(last + 1)

        target = end_epoch or get_system_checkpoint()
        epochs = [EpochNumber(e) for e in range(start_epoch, target + 1)]
        total_epochs = len(epochs)
        
        if not epochs:
            logger.info("No epochs to process")
            return

        logger.info("Processing %d epochs with %d concurrent workers", total_epochs, max_concurrent_epochs)

        completed_epochs = []

        for batch_start_index in range(0, total_epochs, max_concurrent_epochs):
            try:
                batch_results = await process_batch(
                    total_epochs,
                    max_concurrent_epochs,
                    batch_start_index,
                    epochs,
                    final_batch_size,
                    metrics_agent,
                )
                completed_epochs.extend(batch_results.get("completed", []))

            except COMMON_ERRORS as e:
                logger.error(
                    "Failed to process batch starting at index %d: %s",
                    batch_start_index, e
                )
                raise

        flow_end = time.perf_counter()
        elapsed_time = flow_end - flow_start
        logger.info("🏁 Historical sync complete in %.2fs", elapsed_time)

        logger.info("Collecting final metrics after historical sync...")
        try:
            await collect_and_publish_metrics(metrics_agent)
        except COMMON_ERRORS as e:
            logger.warning("Failed to collect final metrics: %s", e)

    except (ConnectionError, TimeoutError, RuntimeError, OSError, asyncio.TimeoutError, ValueError, TypeError) as e:
        logger.error("Critical error in historical sync flow: %s", e)
        raise
    finally:
        logger.info("Collecting final metrics after historical sync...")
        try:
            await collect_and_publish_metrics(metrics_agent)
        except COMMON_ERRORS as e:
            logger.warning("Failed to collect final metrics: %s", e)


async def process_batch(
    total_epochs: int,
    max_concurrent_epochs: int,
    batch_start_index: int,
    epochs: list[EpochNumber],
    batch_size: int,
    metrics_agent: MetricsAgent,
) -> dict[str, Any]:
    """
    Process a batch of epochs concurrently using Prefect's optimized task execution.
    
    This function leverages Prefect's native concurrency for maximum efficiency
    while maintaining proper error handling and progress tracking.
    
    Returns detailed results for each epoch processed.
    """
    logger = get_run_logger()
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

    try:
        await collect_and_publish_metrics(metrics_agent)
        logger.debug("Metrics collection completed for batch %d", batch_number)
    except COMMON_ERRORS as e:
        logger.warning("Failed to collect metrics for batch %d: %s", batch_number, e)

    # Create tasks for all epochs in this batch
    tasks = [sync_epoch(epoch, batch_size) for epoch in batch_epochs]
    
    # Execute all tasks concurrently - let Prefect handle retries automatically
    completed_epochs = await asyncio.gather(*tasks)
    
    batch_end_time = time.perf_counter()
    logger.info(
        "✅ Completed batch %d/%d in %.2fs (processed %d epochs)",
        batch_number,
        total_batches,
        batch_end_time - batch_start_time,
        len(completed_epochs)
    )
    
    return {
        "completed": [{"epoch": epoch, "status": "success"} for epoch in completed_epochs],
        "failed": []  # Prefect handles failures via retries
    }