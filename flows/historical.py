import multiprocessing
import time

from ogmios import Block
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import wait
from prefect_dask import DaskTaskRunner  # type: ignore[attr-defined]

from constants import FIRST_SHELLEY_EPOCH
from client import HecateClient
from sinks.redis import HistoricalRedisSink

from flows import get_system_checkpoint
from models import EpochNumber


@task(cache_policy=NO_CACHE)
async def sync_epoch(
    epoch: EpochNumber,
    batch_size: int,
) -> EpochNumber:
    """
    Synchronize a specific epoch by fetching blocks of data in batches and relaying them.
    The function streams block data from the ledger for a given epoch, sends them to a sink in
    configurable batch sizes and marks the epoch as complete once fully processed.

    :param epoch: The epoch number to synchronize.
    :type epoch: EpochNumber
    :param batch_size: The number of blocks to process in each batch.
    :type batch_size: int
    :return: The updated epoch number indicating the last successfully synced epoch.
    :rtype: EpochNumber
    """
    logger = get_run_logger()
    epoch_start = time.perf_counter()
    logger.info(f"▶️  Starting sync for epoch {epoch}")

    # Fetch _and_ stream blocks concurrently
    async with HistoricalRedisSink() as sink, HecateClient() as client:
        start_height = await sink.get_epoch_resume_height(epoch) or None

        batches_sent = 0
        batch: list[Block] = []
        async for blocks in client.epoch_blocks(epoch):
            for blk in blocks:
                # skip blocks already synced
                if start_height and blk.height <= start_height:
                    continue
                batch.append(blk)
                if len(batch) < batch_size:
                    continue
                # send batch to sink
                batch_start = time.perf_counter()
                await sink.send_batch(batch, epoch=epoch)
                batch_end = time.perf_counter()
                batches_sent += 1
                logger.debug(
                    f" Batch #{batches_sent}: sent {len(batch)} blocks "
                    f"in {batch_end - batch_start:.2f}s"
                )
                batch.clear()

        # finalize partially filled batch, if any
        if batch:
            batch_start = time.perf_counter()
            await sink.send_batch(batch, epoch=epoch)
            batch_end = time.perf_counter()
            batches_sent += 1
            logger.debug(
                f" Final batch #{batches_sent}: sent {len(batch)} blocks "
                f"in {batch_end - batch_start:.2f}s"
            )
        # mark done and advance last_synced_epoch
        new_last = await sink.mark_epoch_complete(epoch)

    epoch_end = time.perf_counter()
    logger.info(
        f"✅ Finished epoch {epoch} in {epoch_end - epoch_start:.2f}s; "
        f"last_synced_epoch → {new_last}"
    )
    return new_last  # type: ignore[no-any-return]


@flow(  # type: ignore[arg-type]
    name="Historical Sync",
    task_runner=DaskTaskRunner(  # type: ignore[arg-type]
        cluster_kwargs={
            "n_workers": multiprocessing.cpu_count(),
            "threads_per_worker": 1,
            "memory_limit": "2GB",
        },
    ),
)
async def historical_sync_flow(
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
    batch_size: int = 1000,
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
    :param batch_size: The number of records processed per batch for synchronization. Defaults to 1000.
    :type batch_size: int
    :return: This flow does not return any value.
    :rtype: None
    """
    logger = get_run_logger()
    flow_start = time.perf_counter()
    async with HistoricalRedisSink(start_epoch=start_epoch) as sink:
        # Here we resume from where we left off or tell redis last_synced_epoch = start_epoch
        last = await sink.get_last_synced_epoch()
    if last > start_epoch:
        logger.info(
            f"🔄 Resuming after last synced epoch {last} instead of {start_epoch}"
        )
        start_epoch = last + 1
    target = get_system_checkpoint()

    epochs = range(start_epoch, target + 1)
    # fire off one sync_epoch per epoch, all at once
    futures = sync_epoch.map(epoch=epochs, batch_size=batch_size)
    # and wait for all to finish
    wait(futures)

    flow_end = time.perf_counter()
    logger.info(f"🏁 Historical sync complete in {flow_end - flow_start:.2f}s")
