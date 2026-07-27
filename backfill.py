"""Historical backfill: fetch a range of epochs from Ogmios into a sink.

This is the library core. It has no orchestration dependency — no flow
engine, no scheduler, no server. Give it an Ogmios endpoint and something
that implements ``send_batch`` and it will relay blocks:

    from backfill import backfill
    from sinks.redis import HistoricalRedisSink

    await backfill(HistoricalRedisSink, start_epoch=208, end_epoch=210)

Epochs are fetched concurrently in separate processes (block parsing is
GIL-bound), then committed *in ascending order* so a downstream consumer
reading epoch-by-epoch never sees a gap.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from contextlib import AsyncExitStack, nullcontext
from dataclasses import dataclass, field
from multiprocessing import cpu_count, get_context
from typing import Any, TypeVar

import ogmios.model.model_map as mm
from ogmios import Block

from client import HecateClient
from config import settings
from config.log import configure_logging
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES, FIRST_SHELLEY_EPOCH
from epoch_cache import extend_cache, load_cache
from epoch_derivation import regenerate_range
from errors import (
    BackfillError,
    EpochsFailedError,
    OrderingStalledError,
    UnreachableWindowError,
    UnsafePurgeError,
)
from models import BlockHeight, EpochData, EpochNumber
from network import NetworkManager
from sinks.base import BlockRelay, EpochCoordinator

logger = logging.getLogger(__name__)

# The library's surface. The errors are re-exported from their home in
# `errors` so that catching what `backfill()` raises needs one import, not two.
__all__ = [
    "BackfillError",
    "EpochJob",
    "EpochsFailedError",
    "OrderingStalledError",
    "SinkFactory",
    "UnreachableWindowError",
    "UnsafePurgeError",
    "async_retry",
    "backfill",
    "default_concurrency",
]

T = TypeVar("T")

#: Builds a fresh, unopened sink. A factory rather than an instance because
#: each epoch is fetched in its own process: the factory is pickled to the
#: worker, which opens its own sink there. A sink class qualifies directly,
#: as does ``functools.partial(SinkClass, prefix="…")``.
SinkFactory = Callable[[], BlockRelay]


def default_concurrency() -> int:
    """Half the CPUs, at least one.

    Concurrency is bounded by memory before it is bounded by cores: every
    epoch in flight is a whole epoch of blocks resident in the sink, on the
    order of a gigabyte. A CPU-count default puts 8–16 epochs in flight on
    hosts that routinely have 16 GB, which overruns the sink long before it
    saturates the cores. Halving trades throughput a caller can opt back
    into via ``--concurrency`` for a default that does not surprise anyone
    downstream.
    """
    return max(1, cpu_count() // 2)


# Attempts after the first, and the wait between them.
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 10.0


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


# Apply performance optimization for Block initialization. Module scope is
# load-bearing: worker processes reach the fetch code by importing this
# module, which is what applies the patch on their side of the fork.
Block.__init__ = fast_block_init


@dataclass(frozen=True, slots=True)
class EpochJob:
    """Everything a worker process needs to relay one epoch.

    Every field has to survive pickling to a spawned process, which is why
    the boundary data travels with the job rather than being looked up in
    the worker: epochs derived on demand are not in the committed CSVs.
    """

    epoch: EpochNumber
    endpoint: str
    batch_size: int
    sink_factory: SinkFactory
    previous_boundary: EpochData
    block_count: int
    retries: int = DEFAULT_RETRIES
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS
    log_level: int = logging.INFO


def _as_coordinator(sink: BlockRelay) -> EpochCoordinator | None:
    """Return the sink if it also coordinates epoch ordering, else None.

    Sinks that only relay blocks are fully supported — they just get no
    resumability, no ordered completion and no backpressure.
    """
    return sink if isinstance(sink, EpochCoordinator) else None


async def async_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    delay_seconds: float,
    label: str,
) -> T:
    """Call ``operation`` until it succeeds, up to ``attempts`` times.

    ``operation`` must be idempotent: ``_sync_epoch`` is, because it clears
    the epoch's sink state before streaming, so a retry always starts from
    a clean slate. ``CancelledError`` is a ``BaseException`` and so passes
    straight through.
    """
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:  # noqa: BLE001 — retry any failed attempt
            if attempt == attempts:
                raise
            logger.warning(
                "%s failed (attempt %d/%d: %s); retrying in %.0fs",
                label,
                attempt,
                attempts,
                exc,
                delay_seconds,
                exc_info=True,
            )
            await asyncio.sleep(delay_seconds)
    raise AssertionError("unreachable: the final attempt either returns or raises")


async def _stream_and_batch_blocks(
    client: HecateClient,
    sink: BlockRelay,
    job: EpochJob,
) -> BlockHeight | None:
    """Stream an epoch's blocks from Ogmios and relay them in batches.

    Returns the height of the last block relayed, or None if the epoch
    yielded nothing.
    """
    batch: list[Block] = []
    last_height: BlockHeight | None = None

    async for blocks in client.epoch_blocks(
        job.epoch,
        previous_boundary=job.previous_boundary,
        block_count=job.block_count,
    ):
        for block in blocks:
            batch.append(block)
            last_height = block.height

            if len(batch) < job.batch_size:
                continue

            await sink.send_batch(batch, epoch=job.epoch)
            logger.debug("Sent batch of %d blocks for epoch %s", len(batch), job.epoch)
            batch.clear()

    # Send any remaining blocks
    if batch:
        await sink.send_batch(batch, epoch=job.epoch)
        logger.debug(
            "Sent final batch of %d blocks for epoch %s", len(batch), job.epoch
        )
        last_height = batch[-1].height
        batch.clear()

    return last_height


async def _sync_epoch(job: EpochJob) -> EpochNumber:
    """Relay every block of one epoch to a freshly opened sink.

    Ordering across epochs is not this function's concern: it writes only
    to its own epoch's destination, and the caller commits epochs in order
    once they land.
    """
    epoch_start = time.perf_counter()
    logger.debug(
        "▶️  Starting sync for epoch %s on endpoint %s", job.epoch, job.endpoint
    )

    async with (
        job.sink_factory() as sink,
        HecateClient(endpoint_url=job.endpoint) as client,
    ):
        # Wipe any stale buffer/resume state so retries always start clean.
        if coordinator := _as_coordinator(sink):
            await coordinator.reset_epoch_state(job.epoch)

        last_height = await _stream_and_batch_blocks(client, sink, job)

        if last_height is None:
            raise ValueError(f"No blocks fetched for epoch {job.epoch}.")

    logger.info(
        "✅ Epoch %s fetched in %.2fs", job.epoch, time.perf_counter() - epoch_start
    )
    return job.epoch


def _sync_epoch_worker(job: EpochJob) -> EpochNumber:
    """Process-pool entry point for one epoch.

    Runs in a spawned process, so it owns its own event loop, sink
    connection and logging configuration; the parent shares nothing with
    it but ``job`` and the environment.
    """
    configure_logging(job.log_level)
    return asyncio.run(
        async_retry(
            lambda: _sync_epoch(job),
            attempts=job.retries + 1,
            delay_seconds=job.retry_delay_seconds,
            label=f"Epoch {job.epoch}",
        )
    )


@dataclass(slots=True)
class _Batch:
    """One concurrent wave of epochs, and where to commit them."""

    jobs: list[EpochJob]
    number: int
    total: int
    concurrency: int
    coordinator: EpochCoordinator | None = None
    failures: dict[EpochNumber, BaseException] = field(default_factory=dict)


async def _fetch_batch(pool: ProcessPoolExecutor, batch: _Batch) -> None:
    """Phase 1: fetch every epoch in the batch concurrently.

    Failures are recorded rather than raised, so the epochs that *did*
    land can still be committed in order before the run gives up.
    """
    loop = asyncio.get_running_loop()
    results = await asyncio.gather(
        *(loop.run_in_executor(pool, _sync_epoch_worker, job) for job in batch.jobs),
        return_exceptions=True,
    )

    for job, result in zip(batch.jobs, results, strict=True):
        if isinstance(result, BaseException):
            batch.failures[job.epoch] = result
            logger.error(
                "❌ Epoch %d failed after %d attempts: %s",
                job.epoch,
                job.retries + 1,
                result,
                exc_info=result,
            )


async def _commit_batch(batch: _Batch) -> None:
    """Phase 2: mark the batch's epochs complete, in ascending order.

    Epochs whose worker raised are deliberately skipped: their partial
    resume height would otherwise publish a truncated epoch as complete.
    """
    coordinator = batch.coordinator
    if coordinator is None:
        return

    for job in batch.jobs:
        if job.epoch in batch.failures:
            continue

        last_height = await coordinator.get_epoch_resume_height(job.epoch)
        if last_height is None:
            logger.warning("Epoch %d has no data, skipping", job.epoch)
            continue

        await coordinator.mark_epoch_complete(job.epoch, last_height)

    await coordinator.note_batch_finished()


async def _run_batch(pool: ProcessPoolExecutor, batch: _Batch) -> None:
    """Fetch a batch of epochs concurrently, then commit them in order."""
    batch_start = time.perf_counter()
    logger.info(
        "🟢 Starting batch %d/%d: epochs %d to %d",
        batch.number,
        batch.total,
        batch.jobs[0].epoch,
        batch.jobs[-1].epoch,
    )

    if coordinator := batch.coordinator:
        await coordinator.wait_for_backpressure()
        await coordinator.note_batch_started(
            active=len(batch.jobs), maximum=batch.concurrency
        )

    await _fetch_batch(pool, batch)
    await _commit_batch(batch)

    logger.info(
        "✅ Completed batch %d/%d in %.2fs",
        batch.number,
        batch.total,
        time.perf_counter() - batch_start,
    )

    if batch.failures:
        raise EpochsFailedError(batch.failures)


async def _resolve_target_and_boundaries(
    end_epoch: EpochNumber | None,
    endpoint: str,
    kupo_url: str | None,
) -> tuple[EpochNumber, dict[EpochNumber, EpochData], dict[EpochNumber, int]]:
    """Determine the seed target from the live chain and assemble boundary data.

    The target is the last finalized epoch (the current open epoch minus one),
    read live from Ogmios rather than a checkpoint file — so a stale checkout can
    no longer silently clamp the seed range. Epochs beyond the frozen bootstrap
    CSVs and the local cache are derived from the chain on demand (kupo-accelerated
    when ``kupo_url`` is set, otherwise a pure chain-sync walk) and cached for reuse.

    Returns the target epoch plus boundary rows and block counts keyed by epoch,
    covering everything the bootstrap, the cache and the derivation know
    through ``target``.
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
                kupo_url=kupo_url,
            )
            boundaries.update(new_rows)
            counts.update(new_counts)
            # Persist the newly derived tail so the next run reuses it.
            extend_cache(new_rows, new_counts)

    return target, boundaries, counts


async def backfill(
    sink_factory: SinkFactory,
    *,
    start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
    end_epoch: EpochNumber | None = None,
    batch_size: int | None = None,
    concurrency: int | None = None,
    endpoints: Sequence[str] | None = None,
    kupo_url: str | None = None,
    rebase_ordering_base: bool = False,
    purge_orphans: bool = False,
    retries: int = DEFAULT_RETRIES,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
    log_level: int = logging.INFO,
) -> EpochNumber | None:
    """Relay every block in an epoch range to the sink, oldest epoch first.

    Epochs are fetched ``concurrency`` at a time, each in its own process,
    then committed in ascending order. If the sink coordinates epochs (see
    ``sinks.base.EpochCoordinator``) the run is resumable — it continues
    after the last epoch already committed — and honours consumer
    backpressure. Otherwise every requested epoch is fetched afresh.

    :param sink_factory: Builds an unopened sink. Called once in this
        process and once per epoch in each worker process, so it must be
        picklable — a sink class or a ``functools.partial`` of one.
    :param start_epoch: First epoch to relay. Ignored in favour of the
        resume point when the sink is further along already. A coordinating
        sink whose delivered mark sits *below* this is refused rather than
        stepped over — see ``rebase_ordering_base``.
    :param end_epoch: Last epoch to relay (inclusive). Defaults to — and is
        clamped to — the last finalized epoch, read live from the chain.
    :param batch_size: Blocks per ``send_batch`` call. Defaults to
        ``BATCH_SIZE`` from the environment.
    :param concurrency: Epochs fetched in parallel. Defaults to half the CPU
        count — see ``default_concurrency``; raise it only against a sink
        sized for the extra resident epochs.
    :param endpoints: Ogmios endpoints to rotate through. Defaults to
        ``OGMIOS_ENDPOINTS`` from the environment.
    :param kupo_url: Optional kupo endpoint, which accelerates deriving
        boundary data for epochs absent from the committed CSVs.
    :param rebase_ordering_base: Allow a window that starts above the sink's
        delivered mark, moving the mark up to meet it. This declares the
        epochs in between out of scope — they will never be delivered from
        this sink — so it is an assertion about intent, not a repair.
    :param purge_orphans: Allow the startup purge to drop already-published
        epochs that no consumer group has registered for. Off by default: a
        consumer that has not started yet is indistinguishable from one that
        never will, and guessing wrong loses data silently.
    :param retries: Extra attempts per epoch after the first failure.
    :param retry_delay_seconds: Wait between attempts.
    :param log_level: Level worker processes configure logging at.
    :return: The last epoch in the relayed range, or None if there was
        nothing to do.
    :raises UnreachableWindowError: if the sink's delivered mark sits below
        the window and ``rebase_ordering_base`` was not given. Nothing is
        written; the run could not have become visible.
    :raises UnsafePurgeError: if reclaiming already-published epochs below the
        window would take data from a consumer. Nothing is deleted.
    :raises EpochsFailedError: if any epoch failed every attempt. Epochs that
        succeeded are committed first, so a rerun resumes cleanly.
    :raises OrderingStalledError: if the relayed epochs did not become
        visible. A bug guard; it should be unreachable.

    All of the above subclass ``errors.BackfillError``.
    """
    run_start = time.perf_counter()
    batch_size = batch_size or settings.BATCH_SIZE
    concurrency = concurrency or default_concurrency()
    network_manager = NetworkManager(endpoints)

    async with AsyncExitStack() as stack:
        # This process only needs a sink of its own to coordinate epochs;
        # a relay-only sink is opened solely inside the workers, so nothing
        # here holds an idle connection a third-party sink would have to
        # keep alive for the length of the run.
        coordinator = _as_coordinator(sink_factory())
        if coordinator is not None:
            await stack.enter_async_context(coordinator)

        if coordinator:
            # Ordered completion counts up one epoch at a time from this base,
            # so the window has to start exactly where the base leaves off.
            base = await coordinator.ensure_ordering_base(EpochNumber(start_epoch - 1))
            rebase_needed = False

            if base >= start_epoch:
                # The sink is already past the requested start — including the
                # case base == start_epoch, where that epoch is done and
                # re-relaying it would strand a duplicate. Warned, not just
                # noted: the caller asked for a window and is getting a
                # narrower one, which is the same class of surprise as the
                # refusal below and deserves the same volume.
                logger.warning(
                    "⚠️  Narrowing the window: epochs %d–%d were already "
                    "delivered here, so this run relays from %d, not %d. A "
                    "consumer re-reading from %d will not receive them again "
                    "— flush the namespace to relay them afresh.",
                    start_epoch,
                    base,
                    base + 1,
                    start_epoch,
                    start_epoch,
                )
                start_epoch = EpochNumber(base + 1)
            elif base < start_epoch - 1:
                if not rebase_ordering_base:
                    raise UnreachableWindowError(base=base, start_epoch=start_epoch)
                rebase_needed = True

            # Purge orphaned epoch streams below start_epoch from prior
            # overlapping runs. Without this, streams that no consumer will
            # ever read (0 consumer groups) block backpressure indefinitely.
            # It also refuses to drop anything a consumer might still be owed,
            # which is what makes the rebase below safe to do after it.
            await coordinator.purge_stale_streams(
                start_epoch, purge_orphans=purge_orphans
            )

            if rebase_needed:
                logger.warning(
                    "⚠️  Moving last_synced_epoch %d → %d: epochs %d–%d are "
                    "written off as out of scope and will never be delivered "
                    "from this sink",
                    base,
                    start_epoch - 1,
                    base + 1,
                    start_epoch - 1,
                )
                await coordinator.reset_ordering_base(EpochNumber(start_epoch - 1))

        target, boundaries, counts = await _resolve_target_and_boundaries(
            end_epoch, network_manager.get_connection(), kupo_url
        )

        epochs = [EpochNumber(e) for e in range(start_epoch, target + 1)]
        if not epochs:
            logger.info("No epochs to process")
            return None

        logger.info(
            "Processing %d epochs (%d..%d) with %d concurrent workers",
            len(epochs),
            epochs[0],
            epochs[-1],
            concurrency,
        )

        total_batches = (len(epochs) + concurrency - 1) // concurrency
        bookkeeping = (
            coordinator.run_bookkeeping(target_epoch=target)
            if coordinator
            else nullcontext()
        )

        # `spawn` keeps workers free of inherited event loops and sockets;
        # each one re-imports this module, applying the Block patch there.
        with ProcessPoolExecutor(
            max_workers=concurrency, mp_context=get_context("spawn")
        ) as pool:
            async with bookkeeping:
                for offset in range(0, len(epochs), concurrency):
                    jobs = [
                        EpochJob(
                            epoch=epoch,
                            endpoint=network_manager.get_connection(),
                            batch_size=batch_size,
                            sink_factory=sink_factory,
                            previous_boundary=boundaries[EpochNumber(epoch - 1)],
                            block_count=counts[epoch],
                            retries=retries,
                            retry_delay_seconds=retry_delay_seconds,
                            log_level=log_level,
                        )
                        for epoch in epochs[offset : offset + concurrency]
                    ]
                    await _run_batch(
                        pool,
                        _Batch(
                            jobs=jobs,
                            number=(offset // concurrency) + 1,
                            total=total_batches,
                            concurrency=concurrency,
                            coordinator=coordinator,
                        ),
                    )

        if coordinator:
            # Never report success while contradicting the field consumers
            # read: every epoch relayed above must be visible by now.
            delivered = await coordinator.get_last_synced_epoch()
            if delivered is None or delivered < target:
                raise OrderingStalledError(last_synced=delivered, target=target)

    logger.info(
        "🏁 Backfill complete through epoch %d in %.2fs",
        target,
        time.perf_counter() - run_start,
    )
    return target
