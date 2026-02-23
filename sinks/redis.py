import asyncio
import importlib
from typing import Any

import orjson as json
from ogmios import Block
from prefect import get_run_logger

from config.settings import redis_settings
from constants import FIRST_SHELLEY_EPOCH
from models import BlockHeight, EpochNumber
from sinks.base import DataSink

_redis_module = importlib.import_module("redis.asyncio")
aioredis = _redis_module


class RedisSink(DataSink):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        prefix: str = "hecate:",
        **redis_kwargs: Any,
    ):
        """Initialize Redis connection."""
        self.redis = aioredis.Redis(host=host, port=port, db=db, **redis_kwargs)
        self.prefix = prefix
        self.block_queue = f"{self.prefix}blocks"
        self.status_key = f"{self.prefix}status"

    async def send_block(self, block: Block) -> None:
        """Send a block to Redis."""
        block_data = await self._prepare_block(block)
        await self.redis.rpush(self.block_queue, json.dumps(block_data))
        await self.redis.hset(self.status_key, "last_block_hash", block_data["hash"])
        await self.redis.hset(
            self.status_key, "last_block_slot", str(block_data["slot"])
        )

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Send a batch of blocks to Redis."""
        if not blocks:
            return

        pipeline = self.redis.pipeline()
        for block in blocks:
            block_data = await self._prepare_block(block)
            await pipeline.rpush(self.block_queue, json.dumps(block_data))

        # Update status with last block info
        last_block = await self._prepare_block(blocks[-1])
        await pipeline.hset(self.status_key, "last_block_hash", last_block["hash"])
        await pipeline.hset(self.status_key, "last_block_slot", str(last_block["slot"]))

        await pipeline.execute()

    async def get_status(self) -> dict[str, Any]:
        status = await self.redis.hgetall(self.status_key)
        queue_length = await self.redis.llen(self.block_queue)

        return {
            "queue_length": queue_length,
            "last_block_hash": status.get(b"last_block_hash", b"").decode(),
            "last_block_slot": status.get(b"last_block_slot", b"0").decode(),
        }

    async def close(self) -> None:
        """Close the Redis connection."""
        await self.redis.close()

    @classmethod
    async def _prepare_block(cls, block: Block) -> dict[str, Any]:
        """
        Prepare a block for sending to Redis.
        This method sends over the entire content of the block in dictionary format,
        minus the schema attribute.
        """
        block_data = {"slot": -1, "hash": block.id}  # We want these fields to be first

        # Filter out unwanted fields from the block
        filtered_block_fields = ("_schematype", "issuer", "id")
        block_data |= {
            field: value
            for field, value in block.__dict__.items()
            if field not in filtered_block_fields
            and field != "transactions"  # Handle txs next
        }

        # Filter out unwanted fields from transactions
        filtered_tx_fields = ("datums", "scripts", "redeemers")
        # noinspection PyTypeChecker
        block_data["transactions"] = [
            {
                field: value
                for field, value in tx.items()
                if field not in filtered_tx_fields
            }
            for tx in block.transactions
        ]

        return block_data


# Lua script to atomically advance last_synced_epoch
_ADVANCE_EPOCH_LUA = r"""
local last_synced_epoch, ready_set, resume_map = KEYS[1], KEYS[2], KEYS[3]
local cur = tonumber(redis.call("GET", last_synced_epoch))
local next = cur + 1
while redis.call("SISMEMBER", ready_set, tostring(next)) == 1 do
  redis.call("SREM", ready_set, tostring(next))
  redis.call("SET", last_synced_epoch, tostring(next))
  redis.call("HDEL", resume_map, tostring(next))
  next = next + 1
end
return redis.call("GET", last_synced_epoch)
"""


class HistoricalRedisSink(DataSink):
    """
    A Redis‐backed DataSink for reliably streaming historical epoch data.

    Each epoch's block batches are written directly to a dedicated per‐epoch
    Redis stream (``<prefix>epoch:{N}``). Control events are logged into a
    separate event stream (``<prefix>event_stream``). Resume positions are
    tracked in a Redis hash (``<prefix>resume_map``) and completed epochs
    awaiting sequential commit live in a Redis set (``<prefix>ready_set``).
    A Lua script atomically advances ``last_synced_epoch`` once all preceding
    epochs are ready, ensuring ordered, at‐least‐once delivery and
    resumability on failure.

    Ordering is guaranteed by construction: each epoch has its own stream,
    and consumers read streams in ascending epoch order.
    """

    def __init__(
        self,
        *,
        start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
        prefix: str = "hecate:history:",
    ):
        self.prefix = prefix

        # Per-epoch stream prefix — each epoch gets its own stream:
        #   hecate:history:epoch:208, hecate:history:epoch:209, …
        self.epoch_stream_prefix = f"{prefix}epoch:"

        # Stream of audit/control events:
        #   • Entries: {"type":"epoch_complete", "epoch":<int>, …}
        self.event_stream = f"{prefix}event_stream"

        # Hash of in‐progress epochs' resume positions:
        #   • Field = epoch_number (as string)
        #   • Value = last_processed_block_height (int)
        self.resume_map = f"{prefix}resume_map"

        # Set of epochs that have been fully processed by a worker,
        # but are waiting for all earlier epochs to complete before
        # advancing `last_synced_epoch`.
        self.ready_set = f"{prefix}ready_set"

        # Highest epoch N such that *all* epochs from start through N
        # have been successfully marked complete and synchronized.
        self.last_synced_epoch = f"{prefix}last_synced_epoch"

        # Lowest epoch whose stream still exists in Redis.
        # Epochs below this have been cleaned up.
        self.low_watermark = f"{prefix}low_watermark"

        self.start_epoch = start_epoch
        self.redis: aioredis.Redis | None = None  # type: ignore
        self._advance_sha: str | None = None
        self.logger = get_run_logger()

    async def __aenter__(self):
        url = redis_settings.url
        self.redis = aioredis.from_url(url, decode_responses=False)
        self.logger.debug("🔗 Connecting to Redis at %s", url)

        # load our Lua once
        self._advance_sha = await self.redis.script_load(_ADVANCE_EPOCH_LUA)
        self.logger.debug("✅ loaded Lua advance script")
        # ensure last_synced_epoch and low_watermark exist
        await self.redis.setnx(self.last_synced_epoch, self.start_epoch - 1)
        await self.redis.setnx(self.low_watermark, self.start_epoch)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.redis:
            await self.redis.close()
            self.logger.debug("🛑 Redis connection closed")

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """XADD a prepared batch payload directly to the per-epoch Redis stream."""
        assert self.redis, "Not initialized"

        epoch = kwargs.pop("epoch")
        last_height = blocks[-1].height

        batch_list = [await self._prepare_block(b) for b in blocks]
        payload = json.dumps(batch_list)

        await self.redis.xadd(
            f"{self.epoch_stream_prefix}{epoch}",
            {"type": "batch", "epoch": str(epoch), "data": payload},
        )

        # Only advance the resume cursor after the XADD is confirmed.
        await self.redis.hset(self.resume_map, epoch, last_height)

        self.logger.debug(
            "Wrote batch for epoch %s, up to height %s", epoch, last_height
        )

    async def reset_epoch_state(self, epoch: EpochNumber) -> None:
        """Clear epoch stream and resume state for an epoch (used before retries)."""
        assert self.redis, "Not initialized"
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(f"{self.epoch_stream_prefix}{epoch}")
        pipe.hdel(self.resume_map, epoch)
        await pipe.execute()

    async def mark_epoch_complete(
        self,
        epoch: EpochNumber,
        last_height: BlockHeight,
    ) -> EpochNumber:
        """
        Mark an epoch as complete by updating Redis with the relevant information
        and logging the event. This function performs two main tasks:

        1. Adds the epoch to a set of completed epochs and logs the completion event,
            including the last processed block height consumers should expect for this epoch.
        2. Executes the Lua script stored in Redis to update the last synced epoch and
            clean up associated resume data.

        This function ensures synchronization of epoch information and related
        cleanup tasks.

        :param epoch: The epoch number that has been completed.
        :type epoch: EpochNumber
        :param last_height: The last block height processed in the associated epoch.
        :type last_height: BlockHeight
        :return: The updated last synced epoch number after Redis executes the Lua script.
        :rtype: EpochNumber
        """
        assert self.redis and self._advance_sha, "Redis not initialized"
        # 1) enqueue into ready_set & log event
        pipe = self.redis.pipeline(transaction=True)
        pipe.sadd(self.ready_set, epoch)
        pipe.xadd(
            self.event_stream,
            {"type": "epoch_complete", "epoch": epoch, "last_height": last_height},
        )
        await pipe.execute()

        # 2) run Lua to bump last_synced_epoch and cleanup resume_map
        latest_epoch = await self.redis.evalsha(
            self._advance_sha,  # Computed SHA1 of the Lua script
            3,  # Number of keys passed to the script
            self.last_synced_epoch,
            self.ready_set,
            self.resume_map,
        )
        last_synced = EpochNumber(int(latest_epoch))
        self.logger.info(
            "Epoch %s marked complete; last_synced_epoch → %s",
            epoch,
            last_synced,
        )
        return last_synced

    async def get_last_synced_epoch(self) -> EpochNumber:
        """Get the last synced epoch from Redis, or `start_epoch` if not set"""
        assert self.redis, "Not initialized"
        val = await self.redis.get(self.last_synced_epoch)
        return EpochNumber(int(val)) if val else self.start_epoch

    async def get_epoch_resume_height(self, epoch: EpochNumber) -> BlockHeight | None:
        assert self.redis, "Not initialized"
        val = await self.redis.hget(self.resume_map, epoch)
        return BlockHeight(int(val)) if val else None

    async def get_low_watermark(self) -> EpochNumber:
        """Get the lowest epoch whose stream still exists in Redis."""
        assert self.redis, "Not initialized"
        val = await self.redis.get(self.low_watermark)
        return EpochNumber(int(val)) if val else self.start_epoch

    async def wait_for_backpressure(self) -> None:
        """Block until consumers have caught up enough to accept more epoch data.

        Compares ``last_synced_epoch`` against ``low_watermark``. If the gap
        exceeds ``max_unconsumed_epochs``, sleeps and retries.
        """
        while True:
            last_synced = await self.get_last_synced_epoch()
            low_wm = await self.get_low_watermark()
            gap = last_synced - low_wm
            if gap < redis_settings.max_unconsumed_epochs:
                return
            self.logger.warning(
                "Backpressure: %d unconsumed epochs (limit %d). Waiting…",
                gap,
                redis_settings.max_unconsumed_epochs,
            )
            await asyncio.sleep(10)

    async def get_status(self) -> dict[str, Any]:
        assert self.redis, "Not initialized"
        return {
            "last_synced_epoch": await self.get_last_synced_epoch(),
            "low_watermark": await self.get_low_watermark(),
            "epochs_individually_completed_pending_sync": await self.redis.scard(
                self.ready_set
            ),
            "epochs_with_active_resume_points": await self.redis.hlen(self.resume_map),
            "redis_connection": "ok",
        }

    async def close(self) -> None:
        await self.__aexit__(None, None, None)

    @classmethod
    async def _prepare_block(cls, block: Block) -> dict[str, Any]:
        """
        Prepare a block for Redis storage by converting it to a dictionary format.

        This method delegates to RedisSink._prepare_block to avoid code duplication,
        accepting a small coupling between sink implementations in exchange for
        maintaining consistency in how blocks are formatted across the application.

        Args:
            block: The Block object to prepare

        Returns:
            dict: A dictionary representation of the block ready for serialization
        """
        return await RedisSink._prepare_block(block)

    async def send_block(self, block: Block) -> None:
        """This method is not used in this class, as we only send batches of blocks instead."""
        raise NotImplementedError
