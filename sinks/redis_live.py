"""The live stream: blocks and rollbacks off the chain tip, for any number of readers.

The layout and its guarantees are the contract in ``docs/live-stream.md``;
this module is the only writer of it. In short, under a namespace ``{ns}``:

* ``{ns}:stream`` — one entry per ``block`` or ``rollback``, in chain order;
* ``{ns}:slots`` — slot-scored index of block entries, ``{hash}:{entry_id}``;
* ``{ns}:state`` — the producer's liveness, node tip and the stream's tail;
* ``{ns}:producer`` — the single-writer lease;
* ``{ns}:consumers`` — consumer anchors, written by consumers, read here.

Every write is one Lua script that first checks this process still holds the
lease and, for a block, that the stream's tail is the block's ancestor. So a
second follower — including one that briefly believed it held the lease — can
never interleave with the first, and no entry can land that does not extend
the one before it.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import socket
import time
import uuid
from collections import OrderedDict
from collections.abc import Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import orjson
from ogmios import Block, Point, Tip

from config import settings
from constants import DEFAULT_LIVE_NAMESPACE, SECURITY_PARAMETER
from errors import LeaseLostError, TailMovedError
from models import BlockHash, BlockHeight, Slot
from sinks.base import prepare_block

_redis_module = importlib.import_module("redis.asyncio")
aioredis = _redis_module

logger = logging.getLogger(__name__)

# See sinks/redis.py: an idle pooled connection is pinged before reuse, so a
# silently dropped one fails fast instead of on the next fenced write.
_HEALTH_CHECK_INTERVAL_SECONDS = 30

#: Blocks per publishing script call. Bounds the size of one call's arguments
#: while still amortising the round trip when catching up.
_PUBLISH_CHUNK = 64

#: Entries read per call when walking the stream back for its canonical tail.
_WALK_PAGE = 256

#: Block heights remembered for filling ``last_height`` after a rollback.
_REMEMBERED_HEIGHTS = 4 * SECURITY_PARAMETER

#: Published blocks between backpressure checks, whatever the clock says.
_BACKPRESSURE_CHECK_BLOCKS = 100

_PROGRESS_REPORT_SECONDS = 10.0


# ---------------------------------------------------------------------------
# Lua. Every script that writes starts with _LEASE_CHECK, so a process that
# lost the lease can never write, however stale its view of the world.
# ---------------------------------------------------------------------------

_LEASE_CHECK = """
local holder = redis.call("GET", KEYS[1])
if holder ~= ARGV[1] then
  return {"lease", holder or ""}
end
"""

_STREAM_IDS = """
local function stream_id(id)
  if id == "" then return 0, 0 end
  local ms, seq = string.match(id, "^(%d+)-(%d+)$")
  if ms == nil then
    ms = string.match(id, "^(%d+)$")
    seq = "0"
  end
  if ms == nil then error("not a stream entry id: " .. id) end
  return tonumber(ms), tonumber(seq)
end

local function id_below(a, b)
  local ams, aseq = stream_id(a)
  local bms, bseq = stream_id(b)
  return ams < bms or (ams == bms and aseq < bseq)
end

local function member_id(member)
  return string.match(member, ":(.+)$")
end
"""

# KEYS: producer, stream, slots, state
# ARGV: producer_id, state_tip_slot, state_tip_height, block_count, then per
#       block: slot, hash, height, ancestor, tip_slot, tip_height, data
_PUBLISH_LUA = (
    _LEASE_CHECK
    + """
local count = tonumber(ARGV[4])
local expected = redis.call("HGET", KEYS[4], "last_hash")
for i = 0, count - 1 do
  local at = 5 + i * 7
  if ARGV[at + 3] ~= expected then
    return {"tail", expected or ""}
  end
  expected = ARGV[at + 1]
end

local ids = {}
for i = 0, count - 1 do
  local at = 5 + i * 7
  local id = redis.call("XADD", KEYS[2], "*",
    "type", "block", "slot", ARGV[at], "hash", ARGV[at + 1],
    "height", ARGV[at + 2], "ancestor", ARGV[at + 3],
    "tip_slot", ARGV[at + 4], "tip_height", ARGV[at + 5], "data", ARGV[at + 6])
  redis.call("ZADD", KEYS[3], ARGV[at], ARGV[at + 1] .. ":" .. id)
  ids[#ids + 1] = id
end

local last = 5 + (count - 1) * 7
redis.call("HSET", KEYS[4],
  "last_slot", ARGV[last], "last_hash", ARGV[last + 1],
  "last_height", ARGV[last + 2],
  "tip_slot", ARGV[2], "tip_height", ARGV[3])
return {"ok", ids}
"""
)

# KEYS: producer, stream, state
# ARGV: producer_id, slot, hash, height, tip_slot, tip_height, expected_tail
_ROLLBACK_LUA = (
    _LEASE_CHECK
    + """
local tail = redis.call("HGET", KEYS[3], "last_hash")
local tail_slot = redis.call("HGET", KEYS[3], "last_slot")
if tail ~= ARGV[7] or tonumber(ARGV[2]) >= tonumber(tail_slot) then
  return {"tail", tail or ""}
end
local id = redis.call("XADD", KEYS[2], "*",
  "type", "rollback", "slot", ARGV[2], "hash", ARGV[3],
  "tip_slot", ARGV[5], "tip_height", ARGV[6])
redis.call("HSET", KEYS[3],
  "last_slot", ARGV[2], "last_hash", ARGV[3], "last_height", ARGV[4],
  "tip_slot", ARGV[5], "tip_height", ARGV[6])
return {"ok", id}
"""
)

# KEYS: producer, stream, state
# ARGV: producer_id, slot, hash, height
_ANCHOR_LUA = (
    _LEASE_CHECK
    + """
if redis.call("XLEN", KEYS[2]) > 0 then
  return {"tail", redis.call("HGET", KEYS[3], "last_hash") or ""}
end
redis.call("HSET", KEYS[3],
  "last_slot", ARGV[2], "last_hash", ARGV[3], "last_height", ARGV[4])
return {"ok"}
"""
)

# KEYS: producer, state
# ARGV: producer_id, lease_ms, heartbeat_ts, started_at, depth, paused,
#       paused_since, tip_slot, tip_height
_HEARTBEAT_LUA = (
    _LEASE_CHECK
    + """
redis.call("PEXPIRE", KEYS[1], ARGV[2])
redis.call("HSET", KEYS[2],
  "producer_id", ARGV[1], "heartbeat_ts", ARGV[3], "started_at", ARGV[4],
  "depth", ARGV[5], "paused", ARGV[6], "paused_since", ARGV[7])
if ARGV[8] ~= "" then
  redis.call("HSET", KEYS[2], "tip_slot", ARGV[8], "tip_height", ARGV[9])
end
return {"ok"}
"""
)

# KEYS: producer
# ARGV: producer_id
_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"""

# KEYS: stream
# ARGV: end (an entry id, "(id" for exclusive, or "+"), count
# Read-only. Returns [id, type, slot, hash, height] per entry, newest first,
# so walking the stream never ships block payloads over the wire.
_WALK_LUA = """
local rows = {}
for _, entry in ipairs(redis.call("XREVRANGE", KEYS[1], ARGV[1], "-", "COUNT", ARGV[2])) do
  local row = {entry[1], "", "", "", ""}
  local fields = entry[2]
  for i = 1, #fields, 2 do
    local name = fields[i]
    if name == "type" then row[2] = fields[i + 1]
    elseif name == "slot" then row[3] = fields[i + 1]
    elseif name == "hash" then row[4] = fields[i + 1]
    elseif name == "height" then row[5] = fields[i + 1]
    end
  end
  rows[#rows + 1] = row
end
return rows
"""

# KEYS: producer, stream, slots, consumers
# ARGV: producer_id, now, retain_blocks, max_retained_blocks,
#       active_consumer_seconds
#
# Slots order the index, not entry ids: a rollback re-publishes lower slots
# after higher ones. So "keep the newest n" is taken as the lowest entry id
# among the n highest-slot members, which keeps at least those n whatever
# order rollbacks left them in — and the index is swept by entry id, not by
# rank, so it loses exactly the members whose entries were trimmed.
_TRIM_LUA = (
    _LEASE_CHECK
    + _STREAM_IDS
    + """
local now = tonumber(ARGV[2])
local retain = tonumber(ARGV[3])
local max_retained = tonumber(ARGV[4])
local active_after = now - tonumber(ARGV[5])

local count = redis.call("ZCARD", KEYS[3])
if count <= retain then
  return {"ok", 0, 0}
end
-- The first consumer to register is owed the stream from its start: until one
-- has, nothing is trimmed, and backpressure bounds how far the stream grows.
if redis.call("HLEN", KEYS[4]) == 0 then
  return {"ok", 0, 0}
end

local function oldest_of_newest(n)
  local oldest = nil
  for _, member in ipairs(redis.call("ZRANGE", KEYS[3], -n, -1)) do
    local id = member_id(member)
    if oldest == nil or id_below(id, oldest) then oldest = id end
  end
  return oldest
end

-- A consumer that has read nothing yet anchors at the block at or before its
-- slot (the lowest entry id there, should a fork have re-published it), or at
-- the stream's start when the stream holds none that early.
local function entry_at_or_before(slot)
  local top = redis.call("ZREVRANGEBYSCORE", KEYS[3], slot, "-inf", "WITHSCORES", "LIMIT", 0, 1)
  if #top == 0 then return "" end
  local oldest = nil
  for _, member in ipairs(redis.call("ZRANGEBYSCORE", KEYS[3], top[2], top[2])) do
    local id = member_id(member)
    if oldest == nil or id_below(id, oldest) then oldest = id end
  end
  return oldest
end

local cut = oldest_of_newest(retain)
local cap = nil
if count > max_retained then
  cap = oldest_of_newest(max_retained)
end

local consumers = redis.call("HGETALL", KEYS[4])
for i = 1, #consumers, 2 do
  local ok, anchor = pcall(cjson.decode, consumers[i + 1])
  if not ok or type(anchor) ~= "table" or type(anchor["updated_at"]) ~= "number"
      or type(anchor["slot"]) ~= "number" then
    return redis.error_reply(
      "consumer " .. consumers[i] .. " registered an unreadable anchor: "
      .. consumers[i + 1])
  end
  local pin = anchor["entry_id"]
  if type(pin) ~= "string" or pin == "" then pin = entry_at_or_before(anchor["slot"]) end
  if anchor["updated_at"] < active_after and cap ~= nil and id_below(pin, cap) then
    pin = cap
  end
  if id_below(pin, cut) then cut = pin end
end

local first = redis.call("XRANGE", KEYS[2], "-", "+", "COUNT", 1)[1]
if first == nil or not id_below(first[1], cut) then
  return {"ok", 0, 0}
end

local removed = redis.call("XTRIM", KEYS[2], "MINID", cut)

local stale = {}
local start = 0
while true do
  local members = redis.call("ZRANGE", KEYS[3], start, start + 999)
  if #members == 0 then break end
  for _, member in ipairs(members) do
    if id_below(member_id(member), cut) then stale[#stale + 1] = member end
  end
  start = start + 1000
end
for i = 1, #stale, 500 do
  redis.call("ZREM", KEYS[3], unpack(stale, i, math.min(i + 499, #stale)))
end
return {"ok", removed, #stale}
"""
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LiveKeys:
    """Where each part of the live stream lives, under one namespace."""

    namespace: str

    @property
    def stream(self) -> str:
        return f"{self.namespace}:stream"

    @property
    def slots(self) -> str:
        return f"{self.namespace}:slots"

    @property
    def state(self) -> str:
        return f"{self.namespace}:state"

    @property
    def producer(self) -> str:
        return f"{self.namespace}:producer"

    @property
    def consumers(self) -> str:
        return f"{self.namespace}:consumers"


@dataclass(frozen=True, slots=True)
class LivePolicy:
    """The producer's knobs: liveness, backpressure and retention.

    Defaults are the contract's. ``retain_blocks`` is the security parameter:
    a consumer restarting anywhere inside the rollback window can still find
    its anchor in the stream.
    """

    #: How long the producer lease lives without a heartbeat renewing it.
    lease_seconds: float = 15.0
    #: How often the lease is renewed and ``{ns}:state`` refreshed.
    heartbeat_seconds: float = 2.0
    #: Block entries the slowest active consumer may trail the tail by.
    max_unconsumed_blocks: int = 10_000
    #: Block entries always kept, however far every consumer has read.
    retain_blocks: int = SECURITY_PARAMETER
    #: Block entries a stale consumer's anchor can hold back from trimming.
    max_retained_blocks: int = 20 * SECURITY_PARAMETER
    #: A consumer that committed this recently is active; older is stale.
    active_consumer_seconds: float = 600.0
    #: How often the stream is trimmed.
    trim_interval_seconds: float = 30.0
    #: How often consumer lag is re-read while relaying; while paused it is
    #: re-read every heartbeat.
    backpressure_check_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        # Three missed heartbeats before the lease lapses: one slow Redis
        # round trip must not hand the stream to a standby.
        if self.lease_seconds < 3 * self.heartbeat_seconds:
            raise ValueError(
                f"lease_seconds ({self.lease_seconds}) must be at least three "
                f"heartbeats ({3 * self.heartbeat_seconds})"
            )
        if self.retain_blocks < 1:
            raise ValueError("retain_blocks must be at least 1")
        if self.max_retained_blocks < self.retain_blocks:
            raise ValueError(
                f"max_retained_blocks ({self.max_retained_blocks}) cannot be "
                f"below retain_blocks ({self.retain_blocks})"
            )
        if self.max_unconsumed_blocks < 1:
            raise ValueError("max_unconsumed_blocks must be at least 1")
        if self.active_consumer_seconds <= 0:
            raise ValueError("active_consumer_seconds must be positive")
        if self.backpressure_check_seconds < 0:
            raise ValueError("backpressure_check_seconds cannot be negative")

    @property
    def lease_ms(self) -> int:
        return int(self.lease_seconds * 1000)


@dataclass(frozen=True, slots=True)
class CanonicalPoint:
    """A point still on the stream's chain once its rollbacks are applied."""

    slot: Slot
    hash: BlockHash
    #: Known for block entries; a rollback entry records no height.
    height: BlockHeight | None

    def to_point(self) -> Point:
        return Point(slot=self.slot, id=self.hash)


@dataclass(frozen=True, slots=True)
class ConsumerAnchor:
    """Where one consumer group last committed, as it registered it."""

    group: str
    slot: Slot
    hash: BlockHash
    #: Empty until the consumer has read an entry of this stream.
    entry_id: str
    updated_at: float

    def is_active(self, now: float, active_seconds: float) -> bool:
        return self.updated_at >= now - active_seconds


def default_producer_id() -> str:
    """Unique per process, and says which process when read off the lease."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class RedisLiveSink:
    """Writes the live stream; the ``RollbackRelay`` behind ``BufferedSink``.

    Holds no chain logic of its own beyond refusing any write that would break
    the stream's order: ``BufferedSink`` decides what to publish, this makes
    each publish atomic, fenced and indexed, and keeps the lease, the
    heartbeat, backpressure and retention running around it.

    Knows the stream's tail as it last wrote it (``tail``). Every write is
    checked against Redis's copy in the same script, so the two can only
    disagree if something else wrote — which ends this producer.
    """

    def __init__(
        self,
        *,
        namespace: str = DEFAULT_LIVE_NAMESPACE,
        url: str | None = None,
        policy: LivePolicy | None = None,
        depth: int = 2,
        producer_id: str | None = None,
    ):
        self.keys = LiveKeys(namespace)
        self.url = url or settings.REDIS_URL
        self.policy = policy or LivePolicy()
        self.depth = depth
        self.producer_id = producer_id or default_producer_id()
        self.redis: Any = None
        #: The stream's canonical tail as this producer last wrote or read it.
        self.tail: Point | None = None

        self._scripts: dict[str, Any] = {}
        self._tip: Tip | None = None
        self._started_at = time.time()
        self._paused_since: float | None = None
        self._heights: OrderedDict[str, BlockHeight] = OrderedDict()
        self._last_check = 0.0
        self._published_since_check = 0
        self._last_report = time.monotonic()
        self._published_since_report = 0

    async def __aenter__(self) -> RedisLiveSink:
        self.redis = aioredis.from_url(
            self.url,
            decode_responses=True,
            health_check_interval=_HEALTH_CHECK_INTERVAL_SECONDS,
        )
        for name, source in (
            ("publish", _PUBLISH_LUA),
            ("rollback", _ROLLBACK_LUA),
            ("anchor", _ANCHOR_LUA),
            ("heartbeat", _HEARTBEAT_LUA),
            ("release", _RELEASE_LUA),
            ("walk", _WALK_LUA),
            ("trim", _TRIM_LUA),
        ):
            self._scripts[name] = self.redis.register_script(source)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None

    # -- the lease ----------------------------------------------------------

    async def try_acquire_lease(self) -> bool:
        """Take the producer lease if nobody holds it."""
        return bool(
            await self.redis.set(
                self.keys.producer, self.producer_id, nx=True, px=self.policy.lease_ms
            )
        )

    async def lease_holder(self) -> str | None:
        holder: str | None = await self.redis.get(self.keys.producer)
        return holder

    async def wait_for_lease(self, stop: asyncio.Event) -> bool:
        """Stand by until the lease is ours, or ``stop`` is set.

        Returns whether the lease was taken. A standby writes nothing: the
        state hash belongs to whoever holds the lease.
        """
        announced: str | None = None
        while not stop.is_set():
            if await self.try_acquire_lease():
                logger.info("Took the producer lease as %s", self.producer_id)
                return True
            holder = await self.lease_holder()
            if holder is not None and holder != announced:
                logger.info(
                    "Standing by: %s holds the producer lease for %s",
                    holder,
                    self.keys.namespace,
                )
                announced = holder
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=self.policy.heartbeat_seconds
                )
        return False

    async def release_lease(self) -> None:
        """Give the lease up, if it is still ours, so a standby need not wait."""
        await self._scripts["release"](
            keys=[self.keys.producer], args=[self.producer_id]
        )

    async def heartbeat(self) -> None:
        """Renew the lease and refresh the producer's fields of ``{ns}:state``.

        :raises LeaseLostError: if the lease lapsed or changed hands.
        """
        tip = self._tip
        result = await self._scripts["heartbeat"](
            keys=[self.keys.producer, self.keys.state],
            args=[
                self.producer_id,
                self.policy.lease_ms,
                time.time(),
                self._started_at,
                self.depth,
                "0" if self._paused_since is None else "1",
                "" if self._paused_since is None else self._paused_since,
                "" if tip is None else tip.slot,
                "" if tip is None else _tip_height(tip),
            ],
        )
        self._check(result)

    async def note_tip(self, tip: Tip) -> None:
        """Record the node's tip, writing it through when it has moved."""
        if tip == self._tip:
            return
        self._tip = tip
        await self.heartbeat()

    async def run_upkeep(self) -> None:
        """Heartbeat and trim, forever. Runs beside the relay, never inside it.

        Separate so that neither a node that has not produced a block for a
        minute nor a pause under backpressure can starve the lease.

        :raises LeaseLostError: the moment the lease is found lost.
        """
        next_trim = time.monotonic()
        while True:
            await self.heartbeat()
            if time.monotonic() >= next_trim:
                entries, members = await self.trim()
                if entries:
                    logger.debug(
                        "Trimmed %d stream entries and %d index members",
                        entries,
                        members,
                    )
                next_trim = time.monotonic() + self.policy.trim_interval_seconds
            await asyncio.sleep(self.policy.heartbeat_seconds)

    # -- reading ------------------------------------------------------------

    async def read_state(self) -> dict[str, str]:
        state: dict[str, str] = await self.redis.hgetall(self.keys.state)
        return state

    async def stream_length(self) -> int:
        return int(await self.redis.xlen(self.keys.stream))

    async def canonical_points(
        self, limit: int = SECURITY_PARAMETER + 1
    ) -> list[CanonicalPoint]:
        """Up to ``limit`` points on the stream's chain, newest (the tail) first.

        Walks the stream backwards applying its rollbacks: a rollback to P
        orphans every earlier-published block above P's slot, and P itself is
        a canonical point. A block re-published after a rollback appears once.
        """
        points: list[CanonicalPoint] = []
        position: dict[str, int] = {}
        floor: int | None = None
        end = "+"
        while len(points) < limit:
            rows = await self._scripts["walk"](
                keys=[self.keys.stream], args=[end, _WALK_PAGE]
            )
            if not rows:
                break
            for entry_id, kind, raw_slot, block_hash, raw_height in rows:
                slot = int(raw_slot)
                canonical = floor is None or slot <= floor
                if kind == "rollback":
                    floor = slot if floor is None else min(floor, slot)
                elif kind != "block":
                    raise ValueError(f"stream entry {entry_id} has type {kind!r}")
                if not canonical:
                    continue

                height = BlockHeight(int(raw_height)) if raw_height else None
                point = CanonicalPoint(Slot(slot), BlockHash(block_hash), height)
                if height is not None:
                    self._remember_height(block_hash, height)
                if block_hash in position:
                    # A rollback point met again as the block it rolled back
                    # to: same point, and now its height is known.
                    if height is not None:
                        points[position[block_hash]] = point
                    continue
                position[block_hash] = len(points)
                points.append(point)
                if len(points) == limit:
                    break
            end = f"({rows[-1][0]}"
        return points

    async def consumer_anchors(self) -> list[ConsumerAnchor]:
        """Every consumer registration, as written by the consumers.

        :raises ValueError: for a registration missing a contract field; the
            producer cannot bound retention or backpressure around a consumer
            whose position it cannot read.
        """
        raw: dict[str, str] = await self.redis.hgetall(self.keys.consumers)
        anchors = []
        for group, value in raw.items():
            try:
                fields = orjson.loads(value)
                entry_id = fields["entry_id"]
                anchors.append(
                    ConsumerAnchor(
                        group=group,
                        slot=Slot(int(fields["slot"])),
                        hash=BlockHash(fields["hash"]),
                        # null is how a consumer anchored outside this stream
                        # (at the end of a backfill, say) says so.
                        entry_id="" if entry_id is None else str(entry_id),
                        updated_at=float(fields["updated_at"]),
                    )
                )
            except (orjson.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"consumer {group} registered an unreadable anchor in "
                    f"{self.keys.consumers}: {value!r} ({exc})"
                ) from exc
        return anchors

    async def unconsumed_blocks(self) -> int:
        """Block entries past the slowest active consumer's anchor.

        Zero when every registered consumer is stale: they stopped reading, and
        holding the chain back for them would stall everyone else. Every block
        entry when none has registered yet: the first to arrive reads the
        stream from its start, so all of it is still owed.
        """
        now = time.time()
        anchors = await self.consumer_anchors()
        if not anchors:
            return int(await self.redis.zcard(self.keys.slots))
        active = [
            anchor
            for anchor in anchors
            if anchor.is_active(now, self.policy.active_consumer_seconds)
        ]
        if not active:
            return 0
        slowest = min(anchor.slot for anchor in active)
        return int(await self.redis.zcount(self.keys.slots, f"({slowest}", "+inf"))

    # -- writing ------------------------------------------------------------

    def adopt_tail(self, tail: Point) -> None:
        """Take ``tail`` as the stream's tail, as read from the stream itself."""
        self.tail = tail

    async def anchor_empty_stream(
        self, point: Point, height: BlockHeight | None = None
    ) -> None:
        """Make ``point`` the tail of a stream with nothing in it yet.

        The first block published must then name ``point`` as its ancestor.

        :raises TailMovedError: if the stream is not empty after all.
        """
        result = await self._scripts["anchor"](
            keys=[self.keys.producer, self.keys.stream, self.keys.state],
            args=[
                self.producer_id,
                point.slot,
                point.id,
                "" if height is None else height,
            ],
        )
        self._check(result, expected=point.id)
        self.tail = point

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Publish ``blocks``, each extending the one before, as block entries.

        ``tip`` (required) is the node's tip as of the reply that delivered
        them, and lands on every entry.

        :raises LeaseLostError: if the lease is no longer this producer's.
        :raises TailMovedError: if the stream's tail is not the first block's
            ancestor; nothing in the batch is written.
        """
        tip: Tip = kwargs["tip"]
        state_tip = self._tip if self._tip is not None else tip
        for chunk in _chunks(blocks, _PUBLISH_CHUNK):
            args: list[Any] = [
                self.producer_id,
                state_tip.slot,
                _tip_height(state_tip),
                len(chunk),
            ]
            for block in chunk:
                args += [
                    block.slot,
                    block.id,
                    block.height,
                    block.ancestor,
                    tip.slot,
                    _tip_height(tip),
                    orjson.dumps(prepare_block(block)),
                ]
            result = await self._scripts["publish"](
                keys=[
                    self.keys.producer,
                    self.keys.stream,
                    self.keys.slots,
                    self.keys.state,
                ],
                args=args,
            )
            self._check(result, expected=chunk[0].ancestor)

            for block in chunk:
                self._remember_height(block.id, BlockHeight(block.height))
            last = chunk[-1]
            self.tail = Point(slot=last.slot, id=last.id)
            self._published_since_check += len(chunk)
            self._report_progress(len(chunk), last, state_tip)

    async def send_rollback(self, point: Point, **kwargs: Any) -> None:
        """Publish a rollback to ``point``: everything after it is orphaned.

        ``tip`` (required) is the node's tip as of the rollback.

        :raises TailMovedError: if the stream's tail is not the one this
            producer last wrote, or ``point`` is not below it.
        """
        assert self.tail is not None, "no tail adopted or written yet"
        tip: Tip = kwargs["tip"]
        height = self._heights.get(point.id)
        result = await self._scripts["rollback"](
            keys=[self.keys.producer, self.keys.stream, self.keys.state],
            args=[
                self.producer_id,
                point.slot,
                point.id,
                "" if height is None else height,
                tip.slot,
                _tip_height(tip),
                self.tail.id,
            ],
        )
        self._check(result, expected=self.tail.id)
        logger.warning(
            "Published a rollback from %s.%s to %s.%s (node tip %s)",
            self.tail.slot,
            self.tail.id,
            point.slot,
            point.id,
            tip.slot,
        )
        self.tail = point

    async def trim(self) -> tuple[int, int]:
        """Drop entries no consumer is owed; returns (entries, index members).

        See the retention section of docs/live-stream.md for the rules.
        """
        result = await self._scripts["trim"](
            keys=[
                self.keys.producer,
                self.keys.stream,
                self.keys.slots,
                self.keys.consumers,
            ],
            args=[
                self.producer_id,
                time.time(),
                self.policy.retain_blocks,
                self.policy.max_retained_blocks,
                self.policy.active_consumer_seconds,
            ],
        )
        self._check(result)
        return int(result[1]), int(result[2])

    # -- backpressure -------------------------------------------------------

    async def wait_for_backpressure(self) -> None:
        """Return once the slowest active consumer is close enough behind.

        Cheap when nothing is behind: lag is only re-read every
        ``backpressure_check_seconds`` or every hundred published blocks.
        While paused, ``{ns}:state`` says so and the heartbeat keeps running.
        """
        if (
            self._published_since_check < _BACKPRESSURE_CHECK_BLOCKS
            and time.monotonic() - self._last_check
            < self.policy.backpressure_check_seconds
        ):
            return
        self._last_check = time.monotonic()
        self._published_since_check = 0

        limit = self.policy.max_unconsumed_blocks
        lag = await self.unconsumed_blocks()
        if lag <= limit:
            return

        self._paused_since = time.time()
        await self.heartbeat()
        logger.warning(
            "Backpressure: the slowest active consumer is %d blocks behind "
            "(limit %d); pausing",
            lag,
            limit,
        )
        while (lag := await self.unconsumed_blocks()) > limit:
            await asyncio.sleep(self.policy.heartbeat_seconds)

        self._paused_since = None
        self._last_check = time.monotonic()
        await self.heartbeat()
        logger.info("Backpressure released: %d blocks unconsumed", lag)

    # -- internals ----------------------------------------------------------

    def _check(self, result: Sequence[Any], *, expected: str | None = None) -> None:
        """Turn a fenced script's refusal into the error that ends the producer."""
        status = result[0]
        if status == "ok":
            return
        if status == "lease":
            raise LeaseLostError(producer_id=self.producer_id, holder=result[1] or None)
        if status == "tail":
            raise TailMovedError(expected=expected or "", found=result[1] or None)
        raise AssertionError(f"unexpected script result {result!r}")

    def _remember_height(self, block_hash: str, height: BlockHeight) -> None:
        self._heights[block_hash] = height
        self._heights.move_to_end(block_hash)
        while len(self._heights) > _REMEMBERED_HEIGHTS:
            self._heights.popitem(last=False)

    def _report_progress(self, published: int, last: Block, tip: Tip) -> None:
        self._published_since_report += published
        now = time.monotonic()
        if now - self._last_report < _PROGRESS_REPORT_SECONDS:
            return
        logger.info(
            "Relayed %d block(s) through slot %s (height %s); node tip at slot %s",
            self._published_since_report,
            last.slot,
            last.height,
            tip.slot,
        )
        self._last_report = now
        self._published_since_report = 0


def _chunks(blocks: list[Block], size: int) -> Iterator[list[Block]]:
    for start in range(0, len(blocks), size):
        yield blocks[start : start + size]


def _tip_height(tip: Tip) -> int | str:
    """The tip's block height, or empty where the node does not track one.

    Ogmios in front of dolos reports every tip at height 0. Only an empty chain
    has its tip there, so 0 is the node saying nothing, and publishing it would
    read as a consumer being level with a tip it trails by days.
    """
    return tip.height if tip.height > 0 else ""
