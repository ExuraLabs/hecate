import asyncio
import time
from typing import Any

from ogmios import Block
import ogmios.model.model_map as mm
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
# from prefect_dask import DaskTaskRunner  # type: ignore[attr-defined]  # Comentado para usar ConcurrentTaskRunner por defecto

from client import HecateClient
from config.settings import get_batch_settings  # , get_dask_settings  # Comentado: ya no usamos Dask
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


@task(
    retries=3,
    retry_delay_seconds=10,
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
    
    :param epoch: The epoch number to synchronize
    :type epoch: EpochNumber
    :param batch_size: Number of blocks to process per batch. Default 1000 when called directly,
     typically matches BASE_BATCH_SIZE from settings (1000 in production).
    :type batch_size: int
    :return: The synchronized epoch number
    :rtype: EpochNumber
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug("▶️  Starting sync for epoch %s", epoch)

    # Apply performance optimization for Block initialization
    Block.__init__ = fast_block_init
    
    # Use HecateClient for production: use_managed_connection=True prefiere ConnectionManager
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
        
        # Reduce batch size for memory efficiency with concurrent threads
        memory_optimized_batch_size = min(initial_batch_size, 250)  # Max 250 blocks per batch
        
        batch: list[Block] = []
        blocks_processed = 0
        last_height: int | None = None

        async for blocks in client.epoch_blocks(epoch):
            for block in blocks:
                if _should_skip_block(block, resume_height):
                    continue
                    
                batch.append(block)
                last_height = block.height

                # Process smaller batches immediately for memory efficiency
                if len(batch) >= memory_optimized_batch_size:
                    try:
                        await sink.send_batch(batch, epoch=epoch)
                        run_logger.debug("Sent batch of %d blocks for epoch %s", len(batch), epoch)
                        blocks_processed += len(batch)
                        
                        # Immediate memory cleanup
                        batch.clear()
                        
                        # Yield control to other threads every batch
                        await asyncio.sleep(0)
                        
                    except Exception as e:
                        run_logger.error("Failed to send batch for epoch %s: %s", epoch, e)
                        # Don't clear batch on failure, will retry
                        raise

                # Additional memory pressure relief every 1000 blocks
                if blocks_processed > 0 and blocks_processed % 1000 == 0:
                    run_logger.debug("Memory relief checkpoint: %d blocks processed for epoch %s", 
                                   blocks_processed, epoch)
                    await asyncio.sleep(0)  # Yield to event loop

        # Send any remaining blocks
        if batch:
            try:
                await sink.send_batch(batch, epoch=epoch)
                run_logger.debug("Sent batch of %d blocks for epoch %s", len(batch), epoch)
                blocks_processed += len(batch)
                if batch:
                    last_height = batch[-1].height
                    
                # Final cleanup
                batch.clear()
                
            except Exception as e:
                run_logger.error("Failed to send final batch for epoch %s: %s", epoch, e)
                raise

        run_logger.info("Completed epoch %s: processed %d blocks, last height %s", 
                       epoch, blocks_processed, last_height)
        return last_height
        
    except Exception as e:
        run_logger.error("Error streaming blocks for epoch %s: %s", epoch, e)
        raise


def _should_skip_block(block: Block, resume_height: BlockHeight | None) -> bool:
    """Check if a block should be skipped based on resume height."""
    return resume_height is not None and block.height <= resume_height


@flow(  # type: ignore[arg-type]
    name="Historical Sync",
    # task_runner=DaskTaskRunner(  # type: ignore[arg-type]  # Comentado para usar ConcurrentTaskRunner por defecto
    #     cluster_kwargs={
    #         "n_workers": get_dask_settings().n_workers,
    #         "threads_per_worker": 1,
    #         "memory_limit": get_dask_settings().worker_memory_limit,
    #     },
    # ),
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

    This asynchronous flow uses the default ConcurrentTaskRunner (instead of Dask) to handle 
    workloads and ensures data is passed along efficiently using defined batch sizes. 
    The execution time is logged to monitor the performance of the process.

    :param start_epoch: The starting epoch for synchronization. Defaults to FIRST_SHELLEY_EPOCH.
    :type start_epoch: EpochNumber
    :param batch_size: The number of blocks processed per batch for synchronization. 
     Defaults to BASE_BATCH_SIZE from settings (typically 1000 in production).
    :type batch_size: int | None
    :param concurrent_epochs: The number of epochs to process concurrently. 
     Defaults to 6 workers using ConcurrentTaskRunner (previously DASK_N_WORKERS).
    :type concurrent_epochs: int | None
    :return: This flow does not return any value.
    :rtype: None
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()

    # Initialize singleton metrics agent for this flow
    metrics_agent = MetricsAgent.get_instance()

    try:
        # Load settings from centralized config if not provided explicitly
        batch_settings = get_batch_settings()
        final_batch_size = batch_size or batch_settings.base_size
        # final_concurrent_epochs = concurrent_epochs or get_dask_settings().n_workers  # Comentado: ya no usamos Dask
        final_concurrent_epochs = concurrent_epochs or 6  # Usar ConcurrentTaskRunner por defecto con 6 workers

        # Initial metrics collection before starting sync
        logger.info("Collecting initial metrics before historical sync...")
        try:
            await collect_and_publish_metrics(metrics_agent)
        except COMMON_ERRORS as e:
            logger.warning("Failed to collect initial metrics: %s", e)

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
                await process_batch(
                    total_epochs,
                    final_concurrent_epochs,
                    batch_start_index,
                    epochs,
                    final_batch_size,
                    metrics_agent,
                )
            except COMMON_ERRORS as e:
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
        # Collect final metrics before closing
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
        metrics_agent: Singleton metrics agent instance for state persistence
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

    # Collect metrics at the start of each batch
    try:
        await collect_and_publish_metrics(metrics_agent)
        logger.debug("Metrics collection completed for batch %d", batch_number)
    except COMMON_ERRORS as e:
        logger.warning("Failed to collect metrics for batch %d: %s", batch_number, e)

    # Execute tasks concurrently - method works at runtime despite linter warnings
    futures = sync_epoch.map(  # type: ignore[attr-defined]
        epoch=batch_epochs,
        batch_size=[batch_size] * len(batch_epochs)
    )
    
    # Wait for all tasks to complete
    wait(futures)

    batch_end_time = time.perf_counter()
    logger.info(
        "✅ Completed batch %d/%d in %.2fs",
        batch_number, total_batches, batch_end_time - batch_start_time
    )