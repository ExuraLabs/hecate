"""Producer metrics for the pipeline dashboard.

This module owns *what* gets published, not the namespace it lives in.
The sink's key prefix is sink knowledge, not metrics knowledge — callers
pass their prefix when constructing a ``MetricsClient`` and the keys
fall out as ``{prefix}metrics`` and ``{prefix}epoch:{N}:meta``.

Counters use HINCRBY so they aggregate cleanly across the historical
flow's worker processes; the dashboard differentiates them over time to
compute rates.

Every public coroutine is best-effort: a Redis hiccup on a metric write
is logged at warning level and swallowed so it can never break the
producer.

==========================================================================
Wire format (consumed by the pipeline dashboard)
==========================================================================

For the historical flow, ``prefix == "hecate:history:"``. All values
below are stored as Redis bulk strings (bytes); decode as UTF-8.

``hecate:history:metrics`` (HSET, snapshot/counter)
    blocks_total                int   monotonic; XADDed blocks across all
                                      workers. Differentiate over time
                                      for blocks/sec.
    epochs_total                int   monotonic; epochs marked complete.
                                      Differentiate for epochs/min.
    last_heartbeat_ts           ISO8601 UTC; written every 1s while the
                                      historical flow is alive. Absence
                                      > ~10s ⇒ "PRODUCER DOWN".
    backpressure_paused         "0" | "1"; "1" while the producer is
                                      blocked waiting on consumer lag.
                                      Written only on real pause/resume
                                      edges, not every poll.
    backpressure_paused_since   ISO8601 UTC at the moment paused→1; ""
                                      otherwise.
    workers_active              int   workers in flight for the current
                                      batch (held through phase-2
                                      mark-complete; drops to 0 between
                                      batches).
    workers_max                 int   configured ceiling
                                      (`concurrent_epochs`).
    last_cleanup_at             ISO8601 UTC of the most recent cleanup
                                      pass (every 45s after a 80s warmup).
    streams_purged_total        int   monotonic; per-epoch streams
                                      deleted by the cleanup loop.

``hecate:history:epoch:{N}:meta`` (HSET, per-epoch, written at completion
                                   except bytes_total which accumulates
                                   per batch during XADD)
    bytes_total                 int   sum of XADD payload bytes (orjson
                                      of the batch list). Proxy for
                                      "how heavy was this epoch".
    first_xadd_ts_ms            int   millisecond prefix of the epoch
                                      stream's first XADD ID — producer
                                      wallclock when batch 1 landed.
    last_xadd_ts_ms             int   …same, for the last XADD. Diff
                                      with first → produce duration.

The ``:meta`` hash is deleted atomically with the corresponding epoch
stream by both purge paths, so the dashboard should treat its disappearance
as "the producer let this one go" rather than "lost data".
"""

import asyncio
import importlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

_redis_module = importlib.import_module("redis.asyncio")
aioredis = _redis_module

Logger = logging.Logger | logging.LoggerAdapter[Any]

# ---------------------------------------------------------------------------
# Internal layout — fields are owned here; key namespacing comes from caller.
# ---------------------------------------------------------------------------

# Snapshot fields under "{prefix}metrics"
_F_BLOCKS_TOTAL = "blocks_total"
_F_EPOCHS_TOTAL = "epochs_total"
_F_LAST_HEARTBEAT = "last_heartbeat_ts"
_F_BACKPRESSURE_PAUSED = "backpressure_paused"
_F_BACKPRESSURE_PAUSED_SINCE = "backpressure_paused_since"
_F_WORKERS_ACTIVE = "workers_active"
_F_WORKERS_MAX = "workers_max"
_F_LAST_CLEANUP_AT = "last_cleanup_at"
_F_STREAMS_PURGED_TOTAL = "streams_purged_total"

# Per-epoch meta fields under "{prefix}epoch:{N}:meta"
_F_BYTES_TOTAL = "bytes_total"
_F_FIRST_XADD_TS_MS = "first_xadd_ts_ms"
_F_LAST_XADD_TS_MS = "last_xadd_ts_ms"

_HEARTBEAT_INTERVAL_SECONDS = 1.0


def epoch_meta_key(prefix: str, epoch: int) -> str:
    """Key under which per-epoch meta lives, given the sink's prefix.

    Exported because the cleanup paths need to delete it alongside the
    epoch stream — they own that lifecycle, not this module — and they
    must follow the same convention MetricsClient writes to.
    """
    return f"{prefix}epoch:{epoch}:meta"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _xadd_ts_ms(entry: Any) -> str:
    entry_id = entry[0]
    if isinstance(entry_id, bytes):
        entry_id = entry_id.decode()
    return str(entry_id).split("-", 1)[0]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class MetricsClient:
    """Bound to a Redis connection + logger for the lifetime of its owner.

    Construct once per owner (typically in the owner's ``__aenter__``) and
    call domain verbs as instance methods. Every method is best-effort —
    failures are logged and swallowed so metrics can never break the
    producer.
    """

    def __init__(self, redis: Any, prefix: str, logger: Logger | None = None):
        self._redis = redis
        self._logger = logger
        self._prefix = prefix
        self._metrics_key = f"{prefix}metrics"

    def _meta_key(self, epoch: int) -> str:
        return epoch_meta_key(self._prefix, epoch)

    @asynccontextmanager
    async def _suppress(self) -> AsyncIterator[None]:
        try:
            yield
        except Exception as e:  # noqa: BLE001
            if self._logger is not None:
                self._logger.warning("metric write failed: %s", e, exc_info=True)

    async def record_batch_written(
        self, *, epoch: int, block_count: int, payload_bytes: int
    ) -> None:
        """A batch of blocks was XADDed for ``epoch``."""
        async with self._suppress():
            pipe = self._redis.pipeline(transaction=True)
            pipe.hincrby(self._metrics_key, _F_BLOCKS_TOTAL, block_count)
            pipe.hincrby(self._meta_key(epoch), _F_BYTES_TOTAL, payload_bytes)
            await pipe.execute()

    async def record_epoch_published(self, *, epoch: int, stream_key: str) -> None:
        """An epoch is marked complete: snapshot its produce window + bump counter.

        XADD IDs are ``<ms>-<seq>``; the millisecond prefix is producer
        wallclock when the batch was XADDed. We record first/last for the
        dashboard's per-epoch produce-duration chart.
        """
        async with self._suppress():
            info: dict[str, Any] = await self._redis.xinfo_stream(stream_key)
            meta_updates: dict[str, str] = {}
            if first_entry := info.get("first-entry"):
                meta_updates[_F_FIRST_XADD_TS_MS] = _xadd_ts_ms(first_entry)
            if last_entry := info.get("last-entry"):
                meta_updates[_F_LAST_XADD_TS_MS] = _xadd_ts_ms(last_entry)

            pipe = self._redis.pipeline(transaction=True)
            if meta_updates:
                pipe.hset(self._meta_key(epoch), mapping=meta_updates)
            pipe.hincrby(self._metrics_key, _F_EPOCHS_TOTAL, 1)
            await pipe.execute()

    async def note_backpressure_pause(self) -> None:
        """Producer just paused due to consumer lag."""
        async with self._suppress():
            await self._redis.hset(
                self._metrics_key,
                mapping={
                    _F_BACKPRESSURE_PAUSED: "1",
                    _F_BACKPRESSURE_PAUSED_SINCE: _utc_now_iso(),
                },
            )

    async def note_backpressure_resume(self) -> None:
        """Producer is no longer paused."""
        async with self._suppress():
            await self._redis.hset(
                self._metrics_key,
                mapping={
                    _F_BACKPRESSURE_PAUSED: "0",
                    _F_BACKPRESSURE_PAUSED_SINCE: "",
                },
            )

    async def note_workers_busy(self, *, active: int, maximum: int) -> None:
        """Phase-1 workers are running for the current batch."""
        async with self._suppress():
            await self._redis.hset(
                self._metrics_key,
                mapping={
                    _F_WORKERS_ACTIVE: str(active),
                    _F_WORKERS_MAX: str(maximum),
                },
            )

    async def note_workers_idle(self) -> None:
        """No workers running between batches."""
        async with self._suppress():
            await self._redis.hset(self._metrics_key, _F_WORKERS_ACTIVE, "0")

    async def note_cleanup_pass(self) -> None:
        """A cleanup loop pass just finished."""
        async with self._suppress():
            await self._redis.hset(
                self._metrics_key, _F_LAST_CLEANUP_AT, _utc_now_iso()
            )

    async def note_stream_purged(self) -> None:
        """One epoch stream was deleted by the cleanup loop."""
        async with self._suppress():
            await self._redis.hincrby(self._metrics_key, _F_STREAMS_PURGED_TOTAL, 1)


@asynccontextmanager
async def heartbeat(
    url: str,
    prefix: str,
    interval_seconds: float = _HEARTBEAT_INTERVAL_SECONDS,
) -> AsyncIterator[None]:
    """Publish a 1Hz liveness pulse for as long as the body runs.

    Owns its own Redis connection because the heartbeat task must outlive
    any sink open/close cycles inside the body. Cancellation is absorbed
    internally — the body sees no ``CancelledError`` from the heartbeat.
    """
    redis: Any = aioredis.from_url(url)
    metrics_key = f"{prefix}metrics"

    async def _beat() -> None:
        while True:
            _ = await redis.hset(metrics_key, _F_LAST_HEARTBEAT, _utc_now_iso())
            await asyncio.sleep(interval_seconds)

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        _ = task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await redis.close()
