# Live Follow

`follow` relays the chain from a start point up to the node's tip, and then
on, one block at a time as the node adopts it, rollbacks included. It is the
counterpart of the [historical backfill](backfill.md), which relays finished
epochs in bulk; like the backfill, it hands what it relays to a sink. The core
is a plain coroutine, `follow()` in [`follow.py`](../follow.py), with a thin
CLI in front of it.

It writes to one sink, the [Redis live stream](live-stream.md), and relies on
that sink beyond delivery: see [Downstream](#downstream).

## Running it

`follow` needs the Redis dependency group (`uv sync --group redis`): the CLI
wires the Redis live sink, and has no option for another.

```bash
# First run on an empty namespace: start right after a known block
uv run python -m cli follow --from-point 150000000.<64-hex block hash>

# ...or at the first block of an epoch, or after a slot, resolved through kupo
uv run python -m cli follow --from-epoch 650 --kupo-url http://localhost:1442
uv run python -m cli follow --from-slot 150000000 --kupo-url http://localhost:1442

# Every later run: no start option. The stream resumes from its own tail.
uv run python -m cli follow

# A second follower on the same namespace waits as standby
uv run python -m cli follow
```

`uv run python -m cli follow --help` lists every option, and
[Start point](live-stream.md#start-point) says how the start options meet
what the stream already holds. SIGTERM or Ctrl-C stops a follower cleanly:
it releases the producer lease so a standby takes over at once, and exits 0.

Connections come from the same environment as the backfill (see
[`config/settings.py`](../config/settings.py)): `OGMIOS_ENDPOINTS` (or repeated
`--endpoint`), `REDIS_URL`, and `KUPO_URL` (or `--kupo-url`) for slot and epoch
starts. Several Ogmios endpoints are used for failover: each reconnect moves to
the next one.

## Exit codes

A supervisor should classify on the exit code; the message is for whoever
reads the journal and may be reworded. The codes will not change.

|Code|Class|What it means|What to do|
|-|-|-|-|
|0|—|Stopped by SIGTERM or SIGINT; the lease was released|—|
|1|—|Anything unclassified: Redis unreachable, an unreadable consumer registration, an Ogmios protocol error|Restart; investigate if it repeats|
|2|—|Usage error: two start options, a slot or epoch start without kupo, knobs that cannot hold together|Fix the command line|
|15|`IntersectionNotFoundError`|The node's tip is past every point offered, and it holds none of them: a different network, or a fork deeper than the points offered|**Do not restart.** Check which chain Ogmios serves|
|16|`FencedOutError` (`LeaseLostError`, `TailMovedError`)|The Redis live sink's [fencing](live-stream.md#fencing) refused a write: this process's lease lapsed or was taken, or the stream's tail is not where it left it|**Restart.** It comes back as standby, and re-reads the tail if it takes over|
|17|`StartPointRefusedError`|No acceptable start: nothing to start from, a start option that contradicts a non-empty stream, kupo unable to place the slot, or further behind the tip than `--max-catchup-epochs`|**Do not restart** unchanged. Fix the start|
|18|`ChainLinkError`|Ogmios sent a block that does not extend the previous one, or a rollback to a point at or past the last block published that is none of the blocks published or held back|**Do not restart** unchanged; report it|

All of these subclass `errors.FollowError` (and `errors.HecateError`, which the
backfill's errors share), so a library caller can catch at whatever
granularity it needs. Codes 10–14 belong to the backfill.

Transport failures are none of these: a dropped or refused Ogmios connection
is retried indefinitely (1s doubling to 30s) without exiting.

## Rollbacks and depth

The relay holds the newest `--depth` blocks (default 2) back from its sink: a
block is delivered once `depth` blocks have been built on it. Most rollbacks at
the tip are one or two blocks deep, and never reach the sink:

- A rollback to a point **among the held blocks**, or to the last block
  delivered, drops the newer held blocks — matched by hash, not just slot —
  and delivers nothing.
- A rollback **below** that empties the buffer and reaches the sink as one
  rollback to the point; the Redis live stream records it as a `rollback`
  entry.
- A rollback to anything else — a slot among the held blocks but another hash,
  or a point past the newest block — is a chain-link violation (exit 18).

Every block is checked to name the previous point as its `ancestor`, both as
it arrives from Ogmios and as it enters the buffer; a block that does not is a
chain-link violation, and nothing from the break onwards is delivered.

Two exceptions cover a node that does not announce what it does, as dolos does
at its tip. A block built on an earlier point the follower relayed within the
last 2160 is a fork switch without its rollback: the rollback to that point is
handled as above, with a warning, before the block. A block identical to the
previous one — dolos sends its tip block again when a session first reaches it
— is skipped.

Blocks held back when a follower stops are not lost: they were never
delivered, and whoever resumes fetches them again from where the sink left
off.

## Downstream

The relay's own pieces are not tied to a sink. The chain-sync loop,
`HecateClient.next_block.follow()`, yields blocks and rollbacks in the order
the node sent them, and `sinks.base.BufferedSink` delivers what survives its
buffer to any `sinks.base.RollbackRelay`: something that takes blocks in chain
order (`send_batch`) and word of a rollback deeper than the buffer
(`send_rollback`). The CLI sink is one. A tip follower that pushed each block
onto a list with `LPUSH`, or published it as a message on a queue, would be as
valid a downstream as the Redis live stream, for whoever needs that shape.

`follow()` is not sink-agnostic: it writes to the
[Redis live stream](live-stream.md) and nothing else, and relies on that sink
for more than delivery. The single-writer lease and the fencing of every write,
resuming from the stream's tail, starting an empty stream at a registered
consumer's anchor, and backpressure are all the Redis live sink's. Relaying
elsewhere means composing the loop and the buffer with another sink, and
answering those questions for it.

## Knobs

The relay's:

|Option|Default|What it bounds|
|-|-|-|
|`--depth`|2|Rollbacks this shallow never reach the sink; adds that many blocks of latency|
|`--max-catchup-epochs`|2|How far behind the tip a start may be|
|`--in-flight`|50|nextBlock requests kept outstanding against Ogmios|

The Redis live sink's, described in [its contract](live-stream.md):

|Option|Default|What it bounds|
|-|-|-|
|`--lease-seconds`|15|How long after the writer stops a standby may take over. At least three heartbeats|
|`--heartbeat-seconds`|2|Lease renewal and `{ns}:state` refresh interval|
|`--max-unconsumed-blocks`|10 000|Backpressure threshold|
|`--retain-blocks`|2160|Block entries always kept|
|`--max-retained-blocks`|43 200|Block entries a stale consumer can pin; at least `--retain-blocks`|
|`--active-consumer-seconds`|600|Active versus stale consumers|

## Using it as a library

```python
import asyncio

from ogmios import Point

from follow import follow

stop = asyncio.Event()  # set it to stop cleanly
await follow(
    namespace="hecate:live",
    start=Point(slot=150_000_000, id="…"),
    endpoints=["ws://localhost:1337"],
    stop=stop,
)
```

`follow()` raises the same `errors.FollowError` classes the CLI maps to exit
codes. For the pieces underneath, and what composing them with another sink
involves, see [Downstream](#downstream).
