import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import ogmios.model.model_map as mm
from ogmios import Block
from prefect import flow, get_run_logger

from client import HecateClient
from config.settings import get_concurrency_settings
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


def sync_epoch_worker(
    epoch: EpochNumber,
    resume_height: BlockHeight | None = None,
) -> dict[str, Any]:
    """
    Worker function that runs in a separate process.
    Must be synchronous and use only serializable arguments.
    
    Args:
        epoch: The epoch number to synchronize
        resume_height: Optional height to resume from if epoch was partially processed
        
    Returns:
        Dictionary with synchronization results and metrics
    """
    # Each process has its own event loop
    return asyncio.run(_async_sync_epoch_worker(epoch, resume_height))


async def _async_sync_epoch_worker(
    epoch: EpochNumber,
    resume_height: BlockHeight | None = None,
) -> dict[str, Any]:
    """
    Async implementation of epoch synchronization for process workers.
    Handles all the actual synchronization logic within a dedicated process.
    """
    try:
        start_time = time.perf_counter()
        blocks_processed = 0
        last_height = BlockHeight(0)
        
        # Apply performance optimization for Block initialization  
        Block.__init__ = fast_block_init
        
        # Create independent connections for this process
        # Note: Each process will create its own Redis connection using settings
        async with HistoricalRedisSink() as sink:
            async with HecateClient(use_managed_connection=False) as client:
                
                # Stream and process blocks for this epoch
                async for blocks in client.epoch_blocks(epoch):
                    # Filter blocks based on resume height
                    filtered_blocks = [
                        block for block in blocks 
                        if resume_height is None or block.height > resume_height
                    ]
                    
                    if filtered_blocks:
                        # Send batch directly without memory control overhead
                        await sink.send_batch(filtered_blocks, epoch=epoch)
                        blocks_processed += len(filtered_blocks)
                        last_height = BlockHeight(filtered_blocks[-1].height)
                
                # Mark epoch as complete
                if last_height > 0:
                    await sink.mark_epoch_complete(epoch, last_height)
                
                elapsed_time = time.perf_counter() - start_time
                
                return {
                    "epoch": epoch,
                    "blocks_processed": blocks_processed,
                    "last_height": last_height,
                    "elapsed_seconds": round(elapsed_time, 2),
                    "status": "success"
                }
                
    except Exception as e:
        return {
            "epoch": epoch,
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__
        }


@flow(name="Historical Sync")
async def historical_sync_flow(
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
    end_epoch: EpochNumber | None = None,
) -> dict[str, Any]:
    """
    Retrieves and relays data across a range of epochs.
    This flow resumes from the last synced epoch if applicable,
    or starts from the specified starting epoch.
    
    Each epoch is processed in a separate process for maximum CPU utilization.
    The number of concurrent processes is determined by concurrency settings.
    
    Args:
        start_epoch: Starting epoch for synchronization
        end_epoch: Ending epoch (if None, uses system checkpoint)
        
    Returns:
        Dictionary containing sync results and metrics
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()

    metrics_agent = MetricsAgent.get_instance()

    try:
        concurrency_settings = get_concurrency_settings()
        max_workers = concurrency_settings.max_workers

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
        epoch_range = list(range(start_epoch, target + 1))
        
        if not epoch_range:
            logger.info("No epochs to process")
            return {"status": "completed", "epochs_processed": 0}

        logger.info("Processing %d epochs with %d parallel processes", len(epoch_range), max_workers)

        # Use ProcessPoolExecutor for true parallelism  
        actual_workers = min(max_workers, len(epoch_range))
        
        with ProcessPoolExecutor(max_workers=actual_workers) as executor:
            loop = asyncio.get_event_loop()
            
            # Get resume heights for all epochs
            async with HistoricalRedisSink() as sink:
                resume_heights = {}
                for epoch in epoch_range:
                    resume_heights[epoch] = await sink.get_epoch_resume_height(EpochNumber(epoch))
            
            # Create futures for each epoch
            futures = []
            for epoch in epoch_range:
                future = loop.run_in_executor(
                    executor,
                    sync_epoch_worker,
                    EpochNumber(epoch),
                    resume_heights.get(epoch)
                )
                futures.append((epoch, future))
            
            # Process all epochs in parallel and collect results
            logger.info(f"Starting {actual_workers} parallel processes for {len(epoch_range)} epochs")
            
            completed_epochs = []
            failed_epochs = []
            total_blocks = 0
            
            for epoch, future in futures:
                try:
                    result = await future
                    if result["status"] == "success":
                        completed_epochs.append(result)
                        total_blocks += result["blocks_processed"]
                        logger.info(f"✅ Epoch {epoch}: {result['blocks_processed']} blocks in {result['elapsed_seconds']}s")
                    else:
                        failed_epochs.append(result)
                        logger.error(f"❌ Epoch {epoch}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    failed_epochs.append({
                        "epoch": epoch,
                        "status": "error",
                        "error": str(e),
                        "error_type": type(e).__name__
                    })
                    logger.error(f"❌ Epoch {epoch} failed: {e}")

        flow_end = time.perf_counter()
        elapsed_time = flow_end - flow_start
        logger.info("🏁 Historical sync complete in %.2fs", elapsed_time)

        # Collect final metrics
        logger.info("Collecting final metrics after historical sync...")
        try:
            await collect_and_publish_metrics(metrics_agent)
        except COMMON_ERRORS as e:
            logger.warning("Failed to collect final metrics: %s", e)

        return {
            "status": "completed" if not failed_epochs else "partial",
            "epochs_processed": len(completed_epochs),
            "epochs_failed": len(failed_epochs),
            "completed_epochs": [r["epoch"] for r in completed_epochs],
            "failed_epochs": [r["epoch"] for r in failed_epochs] if failed_epochs else [],
            "total_blocks": total_blocks,
            "elapsed_seconds": round(elapsed_time, 2),
        }

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