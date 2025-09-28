import time
from typing import Any
from models import BlockHeight

from ogmios import Block
import ogmios.model.model_map as mm
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect_dask import DaskTaskRunner  # type: ignore[attr-defined]

from constants import FIRST_SHELLEY_EPOCH
from client import HecateClient
from sinks.redis import HistoricalRedisSink

from flows import get_system_checkpoint
from flows.adaptive_memory_controller import AdaptiveMemoryController
from models import EpochNumber
from config.settings import (
    get_batch_settings,
    get_dask_settings
)


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
    memory_controller: AdaptiveMemoryController | None = None,
) -> EpochNumber:
    """
    Synchronize a specific epoch by fetching blocks of data in batches and relaying them.
    The function streams block data from the ledger for a given epoch, sends them to a sink in
    configurable batch sizes and marks the epoch as complete once fully processed.
    Dynamically adjusts batch size based on memory pressure using the adaptive memory controller.

    :param epoch: The epoch number to synchronize.
    :type epoch: EpochNumber
    :param batch_size: The initial number of blocks to process in each batch.
    :type batch_size: int
    :param memory_controller: Optional memory controller for adaptive batch sizing.
    :type memory_controller: AdaptiveMemoryController | None
    :return: The updated epoch number indicating the last successfully synced epoch.
    :rtype: EpochNumber
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.debug(f"▶️  Starting sync for epoch {epoch}")

    async with (
        HecateClient() as client,
        HistoricalRedisSink() as sink,
    ):
        original_block_init = Block.__init__
        Block.__init__ = fast_block_init
        try:
            resume_height = await sink.get_epoch_resume_height(epoch)
            batch: list[Block] = []
            blocks_processed = 0
            last_height: int | None = None
            current_batch_size = batch_size

            async for blocks in client.epoch_blocks(epoch):
                for block in blocks:
                    if resume_height and block.height <= resume_height:
                        continue
                    batch.append(block)
                    last_height = block.height
                    
                    # Adapt batch size using memory controller (when needed)
                    if memory_controller and len(batch) % 1000 == 0:  # Check every 1000 blocks
                        current_batch_size = await memory_controller.handle_memory_management(
                            epoch,
                            batch_size,
                            current_batch_size
                        )
                    
                    if len(batch) >= current_batch_size:
                        await sink.send_batch(batch, epoch=epoch)
                        blocks_processed += len(batch)
                        logger.debug(f"Sent batch of {len(batch)} blocks")
                        batch.clear()

            if batch:
                await sink.send_batch(batch, epoch=epoch)
                blocks_processed += len(batch)
                logger.debug(f"Sent final batch of {len(batch)} blocks")
                if batch:
                    last_height = batch[-1].height

            if last_height is None:
                logger.warning(f"No blocks processed for epoch {epoch}, cannot mark complete.")
                return epoch
            await sink.mark_epoch_complete(epoch, BlockHeight(last_height))
            logger.info(f"✅ Epoch {epoch} completed: {blocks_processed} blocks processed")
            return epoch
        finally:
            # Restore original Block.__init__ to avoid side effects for other tasks
            Block.__init__ = original_block_init

    epoch_end = time.perf_counter()
    logger.debug(
        f"✅ Finished epoch {epoch} in {epoch_end - epoch_start:.2f}s"
    )
    return epoch


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
    concurrent_epochs: int | None = None
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

    # Load settings from centralized config if not provided explicitly
    batch_settings = get_batch_settings()
    final_batch_size = batch_size or batch_settings.base_size
    final_concurrent_epochs = concurrent_epochs or get_dask_settings().n_workers
    
    # Create adaptive memory controller for batch size optimization
    memory_controller = AdaptiveMemoryController()

    async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
        last = await sink.get_last_synced_epoch()

    if last > start_epoch:
        logger.info(
            f"🔄 Resuming after last synced epoch {last} instead of {start_epoch}"
        )
    start_epoch = EpochNumber(last + 1)

    target = get_system_checkpoint()
    epochs = list(range(start_epoch, target + 1))
    total_epochs = len(epochs)
    logger.info(
        f"Processing {total_epochs} epochs in batches of {final_concurrent_epochs}"
    )

    for i in range(0, total_epochs, final_concurrent_epochs):
        batch_start = time.perf_counter()
        batch_epochs = epochs[i : i + final_concurrent_epochs]
        batch_num = (i // final_concurrent_epochs) + 1
        total_batches = (
            total_epochs + final_concurrent_epochs - 1
        ) // final_concurrent_epochs

        logger.info(
            (
                f"🔄 Starting batch {batch_num}/{total_batches}: "
                f"epochs {batch_epochs[0]} to {batch_epochs[-1]}"
            )
        )

        futures = sync_epoch.map(
            epoch=batch_epochs, 
            batch_size=final_batch_size,
            memory_controller=memory_controller
        )
        wait(futures)

        batch_end = time.perf_counter()
        logger.info(
            f"✅ Completed batch {batch_num}/{total_batches} in {batch_end - batch_start:.2f}s"
        )

    flow_end = time.perf_counter()
    logger.info(f"🏁 Historical sync complete in {flow_end - flow_start:.2f}s")
