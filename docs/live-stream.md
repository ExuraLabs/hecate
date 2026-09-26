# Redis Live Stream

`RedisLiveSink`, in [`sinks/redis_live.py`](../sinks/redis_live.py), is the
sink the [live follow](follow.md) writes to. It puts what the relay delivers —
blocks off the chain tip, and the rollbacks that
[reach past its buffer](follow.md#rollbacks-and-depth) — into **one Redis
stream per namespace**, which any number of consumers read independently. It
is one downstream shape among others: a list fed with `LPUSH`, or messages on
a queue, would be sinks of their own, with contracts of their own.

This page is that stream's consumer contract, **v1**: the producer is the only
writer, and consumers may rely on everything it states. It also covers what
`follow` relies on this sink for beyond delivery — where a run starts, the
producer lease, backpressure and retention — with each knob named by the
`LivePolicy` field or `follow()` parameter that sets it.

## The stream (contract v1)

Everything lives under a namespace `{ns}`, `hecate:live` by default. It is
independent of the backfill's `hecate:history:*` keys. Every field value is a
string.

### `{ns}:stream` — the entries

A Redis stream, in chain order, one entry per event.

A **block** entry:

|Field|Value|
|-|-|
|`type`|`block`|
|`slot`, `hash`, `height`|The block's, decimal and hex|
|`ancestor`|Hash of the previous block, from the block header|
|`tip_slot`, `tip_height`|The node's tip as of the reply that delivered this block. `tip_height` is empty where the node reports none (dolos reports 0)|
|`data`|`orjson.dumps(prepare_block(block))`: **one** block dict, in the backfill's wire format — not a list, unlike the per-epoch streams|

A **rollback** entry:

|Field|Value|
|-|-|
|`type`|`rollback`|
|`slot`, `hash`|The point the chain rolled back **to**. That block stays canonical; every entry published after it is orphaned|
|`tip_slot`, `tip_height`|The node's tip as of the rollback; `tip_height` as for a block entry|

**The order invariant:** every block entry's `ancestor` equals the `hash` of
the entry immediately before it — a block entry's hash, or a rollback entry's
point. The first block of a stream names its start point. The producer
enforces this atomically at write time (see [fencing](#fencing)); consumers
should verify it on every block regardless.

The same block can appear more than once: published, rolled back, published
again once the chain returns to it. A resumed follower re-publishes whatever
it rolls back over, too.

### `{ns}:slots` — the index

A sorted set: score = slot, member = `{hash}:{entry_id}`, one member per
**block** entry (rollback entries are not indexed). It is trimmed in the same
script as the stream, so it never names a trimmed entry and never misses a
present one. Where one hash has several members, the **last** entry id is the
live one; a member whose entry is gone from the stream is to be treated as
absent.

### `{ns}:state` — the producer's view

|Field|Meaning|
|-|-|
|`producer_id`|The follower holding the lease: `host:pid:random`|
|`heartbeat_ts`|Unix seconds (float) of the last heartbeat|
|`started_at`|Unix seconds this producer started|
|`tip_slot`, `tip_height`|The node's tip: written whenever it moves, and on every heartbeat; `tip_height` as for a block entry|
|`last_slot`, `last_hash`, `last_height`|The stream's canonical tail after its rollbacks. After a rollback entry, its point, with `last_height` empty unless the producer knew that block's height|
|`depth`|The producer's buffer depth|
|`paused`|`1` while paused under backpressure, else `0`|
|`paused_since`|Unix seconds the pause began, or empty|

A heartbeat older than a few `heartbeat_seconds` means no producer is
alive. `tip_slot` far ahead of `last_slot` means one is catching up.

### `{ns}:producer` — the lease

The single-writer lease; its value is the holder's `producer_id`. Taken with
`SET NX PX`, renewed with each heartbeat by a script that re-sets its expiry
only if the value is still this producer's. A follower that cannot take it
waits as **standby**, polling each heartbeat interval, and takes over when it
expires or is released. A standby writes nothing.

### `{ns}:consumers` — the anchors

A hash, one field per consumer group, value JSON:

```json
{"slot": 150000400, "hash": "…", "entry_id": "1727000000000-0", "updated_at": 1727000000.5}
```

Each consumer writes its own after every commit: the point it has applied
through, the stream entry that point came from, and when. **The producer only
reads it**: to pick a start point, and for backpressure and retention.

A consumer that has not read an entry of this stream yet — one anchored at the
end of a backfill, say — writes `entry_id` as `""` or `null`. While active, it
pins the block entry at or before its `slot` against trimming, or the whole
stream when the stream holds nothing that early. One waiting ahead of the
stream pins nothing but what `retain_blocks` keeps anyway.

A registration the producer cannot read (not JSON, or missing `slot`, `hash`,
`entry_id` or `updated_at`) stops the producer: it cannot bound
retention around a consumer whose position it does not know.

### Fencing

Every write the producer makes to `{ns}:stream`, `{ns}:slots` or
`{ns}:state` is **one Lua script** that first checks `GET {ns}:producer` is
still its own `producer_id`, and, for a block, that `state.last_hash` is the
block's `ancestor` (for a rollback, that the tail is the one this producer
last wrote and the point lies below it). Either check failing aborts the write
before anything changes, and the producer stops with `FencedOutError`.

Two followers can momentarily see two different forks, and a stalled follower
can resume believing it still holds a lease that lapsed. The lease plus the
link check make a second writer impossible in both cases: the stale one's next
write is refused, whatever it believes.

## Start point

On its first connection a follower picks where to start, in this order:

1. **A non-empty stream resumes from its canonical tail.** An explicit start
   is then accepted only if it names that exact tail; any other point is
   refused (`StartPointRefusedError`), because publishing from it would break
   the order invariant. So an explicit start is for the first run of a
   namespace only.
2. **An empty stream with an explicit start** begins there. The first block
   published is the one right after the point, and names it as `ancestor`.
3. **An empty stream with registered consumers** begins at the lowest-slot
   anchor among them, active or stale.
4. **Otherwise** it is refused (`StartPointRefusedError`).

It then asks Ogmios to intersect:

- On a non-empty stream it offers the stream's **canonical points**, newest
  first: the stream is walked back applying its rollbacks, and every one of the
  newest 16 is offered, then every power-of-two-th point back to the security
  parameter (2160 blocks), then the oldest reached. The intersection is then at
  most about twice as deep as the real fork.
- If the intersection is **below the tail**, the tail was orphaned while
  nobody was relaying. A `rollback` entry to the intersection is published
  first, then relaying resumes from there.
- If the node holds none of the points **and its tip is past them**, it stops
  with `IntersectionNotFoundError`. If its tip is **behind** the newest point
  offered, the node is still syncing: the follower waits and asks again every
  10 seconds, writing nothing. The same holds for an intersection below the
  tail while the node's tip is behind the tail: a node that has not reached
  the tail yet is not evidence of a fork.
- `max_catchup_epochs` (default 2) bounds how far behind the node's tip the
  start may be. It applies when a follower starts, including when it resumes
  a stream after downtime; reconnects while running are not bounded. Past it,
  the start is refused (`StartPointRefusedError`) before anything is written:
  the live path is one entry per block for every consumer to replay, and a
  stretch of epochs is what the backfill is for. Raise it deliberately for a
  single start.

## Backpressure

A consumer is **active** if its `updated_at` is within
`active_consumer_seconds` (default 600). The producer pauses fetching while
the **slowest active consumer's anchor** is more than `max_unconsumed_blocks`
(default 10 000) block entries behind the tail, counted as the `{ns}:slots`
members above the anchor's slot. While paused it sets `paused=1` and
`paused_since`, keeps heartbeating, and re-reads the lag every heartbeat.

Stale consumers never cause a pause. Lag is re-read every second or every 100
published blocks, so a follower catching up can overshoot the limit by up to
that much before it pauses.

A stream **no consumer has registered on** counts every block entry as
unconsumed: it pauses once it holds `max_unconsumed_blocks`, and resumes
when the first consumer registers. So a follower started ahead of its
consumers waits for them instead of racing ahead of what retention keeps.

## Retention

Trimming runs every 30 seconds, as one script, under these rules:

- **Nothing while no consumer has registered.** The first one to arrive reads
  the stream from its start; backpressure (above) bounds the stream meanwhile.
- **Never past an active consumer's anchor entry** — for one that has read
  nothing yet, the block entry at or before its slot. The anchor entry itself
  is kept, so a restarting consumer can find it.
- **Always at least `retain_blocks`** (default 2160, the security parameter)
  block entries behind the tail — a consumer restarting anywhere inside the
  rollback window can still anchor.
- **A stale consumer pins at most `max_retained_blocks`** (default 43 200,
  about two epochs) block entries. Beyond that it is trimmed past, and the
  consumer, finding its anchor gone, must refuse to resume from it and say so.

Precisely: rollbacks leave slot order and entry order disagreeing, so "the
newest N block entries" is taken as the lowest entry id among the N
highest-slot members of `{ns}:slots`, which keeps at least those N. The cut is
that id for `retain_blocks`, lowered to each active anchor, and lowered to each
stale anchor but no lower than the same bound for `max_retained_blocks`. The
stream is trimmed with `XTRIM MINID` at the cut, and every index member whose
entry id is below the cut is removed in the same script.

## Consumer obligations

The contract, from the reading side:

- Verify the order invariant on every block entry; apply a `rollback` entry by
  undoing everything after its point.
- Take the **last** entry id for a hash in `{ns}:slots`, and treat a member
  whose entry no longer exists as absent.
- Write your anchor to `{ns}:consumers` after every commit. Silence for
  `active_consumer_seconds` makes you stale: you stop holding the producer
  back, and your data is kept only up to `max_retained_blocks`.
- On restart, refuse to anchor at an entry that has been trimmed, and say so.
- Watch `heartbeat_ts`: a stream whose producer has stopped heartbeating is
  not going to grow.
