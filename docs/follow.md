# Live Follow

`follow` relays the chain from a start point up to the node's tip, and then
on, one block at a time as the node adopts it, rollbacks included. It is the
counterpart of the [historical backfill](backfill.md), which relays finished
epochs in bulk; like the backfill, it hands what it relays to a sink. The core
is a plain coroutine, `follow()` in [`follow.py`](../follow.py).

It writes to one sink, the [Redis live stream](live-stream.md), and relies on
that sink beyond delivery: see [Downstream](#downstream).

## Rollbacks and depth

The relay holds the newest `depth` blocks (default 2) back from its sink: a
block is delivered once `depth` blocks have been built on it. Most rollbacks at
the tip are one or two blocks deep, and never reach the sink:

- A rollback to a point **among the held blocks**, or to the last block
  delivered, drops the newer held blocks — matched by hash, not just slot —
  and delivers nothing.
- A rollback **below** that empties the buffer and reaches the sink as one
  rollback to the point; the Redis live stream records it as a `rollback`
  entry.
- A rollback to anything else — a slot among the held blocks but another hash,
  or a point past the newest block — is a chain-link violation
  (`ChainLinkError`).

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

`follow()` stops with an `errors.FollowError` when it cannot carry on. For the
pieces underneath, and what composing them with another sink involves, see
[Downstream](#downstream).
