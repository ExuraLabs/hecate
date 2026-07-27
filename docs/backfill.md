# Historical Backfill

Hecate's backfill relays a range of past epochs from Ogmios to a sink. It is a
plain `asyncio` coroutine — `backfill()` in [`backfill.py`](../backfill.py) —
with a thin CLI in front of it. There is no flow engine, scheduler or server
involved.

## Execution

```bash
# Seed everything from the first Shelley epoch to the chain's last finalized epoch
uv run python -m cli backfill

# A bounded range, six epochs at a time
uv run python -m cli backfill --start-epoch 208 --end-epoch 320 --concurrency 6

# No Redis needed: print what would be relayed
uv run python -m cli backfill --start-epoch 208 --end-epoch 208 --sink cli

# How far along is the Redis sink?
uv run python -m cli status
```

`uv run python -m cli backfill --help` lists every option.

### Sinks

| `--sink` | Destination | Ordering contract |
|---|---|---|
| `redis` (default) | Per-epoch streams `<prefix>epoch:{N}` | Ordered, resumable, backpressured — the full [`EpochCoordinator`](../sinks/base.py) protocol, which a consumer opts into |
| `redis-list` | One list, `<prefix>blocks` | None. Blocks are `RPUSH`ed in order for any `LPOP`/`BRPOP` consumer; no epoch bookkeeping |
| `cli` | Pretty-printed to the terminal | None. For eyeballing a range |

Only `redis` is resumable, because resumability is a property of the sink, not
of the backfill: it is the sink that remembers how far it got. The other two
re-fetch every epoch they are asked for.

Configuration comes from the environment (see [`config/settings.py`](../config/settings.py)),
optionally seeded from a dotenv file: `.env` at the repo root by default, or
whatever `HECATE_ENV_FILE` points at. Real environment variables always win over
dotenv entries. Any `.env*` file is gitignored.

| Variable | Default | Meaning |
|---|---|---|
| `OGMIOS_ENDPOINTS` | `["ws://localhost:1337"]` | JSON array of endpoints, rotated round-robin |
| `REDIS_URL` | `redis://localhost:6379/0` | Where the Redis sink writes |
| `REDIS_MAX_UNCONSUMED_EPOCHS` | `10` | Backpressure threshold |
| `BATCH_SIZE` | `500` | Blocks per batch sent to the sink |
| `KUPO_URL` | — | Accelerates on-demand epoch derivation |

## How It Works

- **Concurrent, in processes**: epochs are fetched `--concurrency` at a time,
  each in its own process from a `ProcessPoolExecutor`. Block parsing is
  GIL-bound, so processes (not tasks) are what buy the parallelism.
- **Two phases per batch**: phase 1 fetches every epoch in the batch
  concurrently and writes blocks straight to their destination; phase 2 marks
  them complete *in ascending order*, advancing `last_synced_epoch` atomically
  via a Lua script.
- **Per-Epoch Streams**: each epoch's block data goes to a dedicated Redis
  stream (`epoch:{N}`). Ordering is guaranteed by construction — consumers read
  epoch streams in ascending order.
- **Resumable**: the run continues after the last epoch already committed, so
  rerunning after a failure picks up where it left off. Progress lives entirely
  in Redis, not in any orchestrator's database.
- **Windows must be contiguous with what the sink has delivered.** See below.
- **Retried**: each epoch gets 3 further attempts, 10s apart. Every attempt
  first clears that epoch's stream and resume cursor, so a retry always starts
  from a clean slate rather than appending to a partial epoch.
- **Fails without publishing garbage**: an epoch that exhausts its retries is
  never marked complete, even though partial blocks may have been written. Its
  siblings in the batch are still committed in order, then the run raises
  `EpochsFailedError`. `last_synced_epoch` therefore always marks a contiguous,
  consumable prefix, and a rerun re-fetches the failed epoch from scratch.
- **Never reports success it cannot back up**: before logging completion, the
  run checks that `last_synced_epoch` actually reached the epoch it relayed to.
  If it did not, it raises `OrderingStalledError` rather than exiting 0 while
  consumers can see nothing.
- **Backpressure**: before each batch, the producer checks how many epoch
  streams are unconsumed. If the gap reaches `REDIS_MAX_UNCONSUMED_EPOCHS` it
  pauses until consumers catch up.
- **Stream Cleanup**: a background task deletes epoch streams once every
  consumer group has acknowledged every entry, advancing `low_watermark`.
- **Fast block construction**: historical blocks bypass Pydantic validation
  (`fast_block_init`), which is redundant for data already on chain.

## Bounded windows and the ordering base

`last_synced_epoch` is the field consumers bound their reads by, and ordered
completion advances it **one epoch at a time** — a Lua script walks upward from
it through `ready_set`. So it can never step over a gap, and a run whose window
starts above it would publish epochs that sit ready but unreachable forever.

The rule is therefore: **a run relaying from epoch `N` requires
`last_synced_epoch == N - 1`.** Three cases, all handled up front, before
anything is written:

| Sink state | What happens |
|---|---|
| `last_synced_epoch` unset | Seeded to `N - 1`. Fresh namespace, nothing to reconcile |
| `last_synced_epoch >= N` | Already delivered that far; the run resumes from `last_synced_epoch + 1` and logs that it did |
| `last_synced_epoch < N - 1` | **Refused** with `UnreachableWindowError`, naming the gap. Nothing is relayed |

For chunked orchestration this is invisible as long as chunk windows are
contiguous: chunk `[501, 503]` leaves the base at 503, and chunk `[504, …]`
lines up exactly.

To relay a **deliberately disjoint** window — declaring the epochs in between
out of scope, never to be delivered from this sink — pass
`--rebase-ordering-base`. It moves the base up to `--start-epoch - 1` *after*
the startup purge has confirmed nothing below the window is still owed to a live
consumer group, and logs a warning naming the epochs written off. It is an
assertion about intent, not a repair.

`python -m cli status` is read-only, and reports `ordering consistent`. That
checks the one invariant this can violate: `low_watermark` may sit at most one
epoch above `last_synced_epoch`, since nothing is reclaimable before it has been
delivered. If it reports inconsistent, epochs are stranded in `ready_set` —
flush the namespace, or relay from `last_synced_epoch + 1`.

## Stream cleanup, and what happens when consumers trail

The cleanup loop runs for the duration of the relay and is cancelled when
relaying ends — **it does not wait for consumers to drain.** That is
deliberate: the producer's job is to relay, and blocking its exit on consumer
progress would stall any orchestrator that runs bounded chunks and needs the
process to exit before it can checkpoint.

The consequence for a consumer that trails the producer is that `low_watermark`
stops tracking its acknowledgements once the run ends, and streams it finishes
with afterwards are reclaimed by the **next** run's `purge_stale_streams` — which
deletes only streams that are missing, orphaned, or fully consumed, and raises
rather than dropping anything a live consumer group is still owed. So a chunked
pipeline should budget for roughly one chunk of streams resident at a time.

## Redis Integration & State Management

- **Redis** manages:
  - Last synced epoch
  - Resume maps per epoch
  - Per-epoch data streams and control events
- **Key Structure** (default prefix `hecate:history:`):
    - `<prefix>epoch:{N}`: Per-epoch Redis stream containing block-batch payloads
    - `<prefix>last_synced_epoch`: Highest sequentially completed epoch
    - `<prefix>low_watermark`: Lowest epoch whose stream still exists in Redis
    - `<prefix>ready_set`: Set of completed epochs awaiting sequential commit
    - `<prefix>resume_map`: Hash of in-progress epochs' resume positions
    - `<prefix>event_stream`: Stream of audit/control events (e.g. `epoch_complete`)
    - `<prefix>metrics`, `<prefix>epoch:{N}:meta`: dashboard signal, documented
      in [`sinks/metrics.py`](../sinks/metrics.py)

## Using it as a library

`backfill()` takes a **sink factory** and nothing else is required. A third
party integrating Hecate implements one method:

```python
import asyncio
from typing import Any

from ogmios import Block

from backfill import backfill
from sinks.base import prepare_block


class MySink:
    """The whole contract: an async context manager that takes batches."""

    async def __aenter__(self) -> "MySink":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        pass

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        epoch = kwargs["epoch"]
        for block in blocks:
            ...  # prepare_block(block) gives you Hecate's wire format


asyncio.run(backfill(MySink, start_epoch=208, end_epoch=210))
```

A factory rather than an instance because each epoch is fetched in its own
process: the factory is pickled to the worker, which opens its own sink there.
A sink class qualifies directly, as does `functools.partial(MySink, ...)`.

Sinks that additionally implement `sinks.base.EpochCoordinator` — as
`HistoricalRedisSink` does — get resumability, ordered epoch completion and
backpressure. A relay-only sink simply fetches every epoch it was asked for.
That split is the whole seam: `send_batch` is the generic relay surface, the
coordinator is the durability surface.

## Epoch Data

Epoch boundaries (per-epoch start/end block height, slot, hash) and block counts
are derived directly from the chain over Ogmios (`epoch_derivation.py`), using an
optional kupo endpoint (`KUPO_URL`) to accelerate boundary lookups. The backfill
reads the last finalized epoch live from Ogmios and derives any epoch beyond
the frozen `data/*.csv` bootstrap on demand, caching the result locally
(`epoch_cache.py`; path configurable via `HECATE_EPOCH_CACHE`).

Nothing maintains the bootstrap CSVs; they are an anchor index, and every epoch
past them comes off the chain. `epoch_derivation.regenerate_range()` is what
would build a fresher snapshot if one is ever wanted — it is the same code path
the backfill uses on demand, so it is exercised on every run past the snapshot.
