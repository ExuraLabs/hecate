import importlib
from typing import Any

import orjson as json
from ogmios import Block
from prefect import get_run_logger

from config.settings import RedisSettings
from constants import FIRST_SHELLEY_EPOCH
from models import BlockHeight, EpochNumber
from sinks.base import DataSink
from sinks.backpressure_monitor import (
    RedisBackpressureConfig,
    RedisBackpressureMonitor,
)

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
        """Initialize Redis connection"""
        self.redis = aioredis.Redis(host=host, port=port, db=db, **redis_kwargs)
        self.prefix = prefix
        self.block_queue = f"{self.prefix}blocks"
        self.status_key = f"{self.prefix}status"

    async def send_block(self, block: Block) -> None:
        """Send a block to Redis"""
        block_data = await self._prepare_block(block)
        await self.redis.rpush(self.block_queue, json.dumps(block_data))
        await self.redis.hset(self.status_key, "last_block_hash", block_data["hash"])
        await self.redis.hset(
            self.status_key, "last_block_slot", str(block_data["slot"])
        )

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        """Send a batch of blocks to Redis"""
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
    A Redis‐backed DataSink for reliably streaming and tracking historical epoch data.

    This sink writes block batches into a Redis Stream (`<prefix>data_stream`) for downstream
    consumers and logs control events (batch sent, epoch complete) into a separate Redis Stream
    (`<prefix>event_stream`). It maintains per‐epoch resume positions in a Redis hash
    (`<prefix>resume_map`) and tracks completed epochs awaiting sequential commit in a Redis set
    (`<prefix>ready_set`). A Lua script atomically advances the `last_synced_epoch` counter
    (`<prefix>last_synced_epoch`) once all preceding epochs are ready, ensuring ordered,
    at‐least‐once delivery and resumability on failure.

    Parameters:
        start_epoch (EpochNumber):
            The initial epoch boundary; used to initialize `last_synced_epoch` if the key is missing.
        prefix (str):
            Redis key namespace prefix (default "hecate:history:").
        max_data_batches (int):
            Max number of block‐batch entries to retain in the data stream (default 10000).
        max_event_entries (int):
            Max number of event entries to retain in the event stream (default 10000).

    Methods:
        __aenter__ / __aexit__:
            Manage the Redis connection lifecycle and load the advance‐script.
        send_batch(epoch, blocks):
            Atomically push a batch to the data stream, emit an event, and update resume position.
        mark_epoch_complete(epoch) -> EpochNumber:
            Mark an epoch ready, log the event, and advance `last_synced_epoch` via lua script.
        get_last_synced_epoch() -> EpochNumber:
            Retrieve the current `last_synced_epoch` value from Redis, or start_epoch if missing.
        get_epoch_resume_height(epoch) -> BlockHeight | None:
            Retrieve the last processed block height for an in‐progress epoch.
        get_status() -> dict:
            Return health and progress metrics (e.g. synced epoch, pending counts, connection status).
        close():
            Alias for __aexit__; cleanly close the Redis connection.
    """

    def __init__(
        self,
        *,
        start_epoch: EpochNumber = FIRST_SHELLEY_EPOCH,
        prefix: str = "hecate:history:",
        max_data_batches: int = 10000,
        max_event_entries: int = 10000,
        backpressure_config: RedisBackpressureConfig | None = None,
        redis_settings: RedisSettings | None = None,
    ):
        self.prefix = prefix

        # Stream of block‐batch payloads:
        #   • Entries: {"type":"batch", "epoch":<int>, "data":<orjson bytes>}
        #   • Trimmed to the most recent `max_data_batches` batches.
        self.data_stream = f"{prefix}data_stream"

        # Stream of audit/control events:
        #   • Entries: {"type":"batch_sent"|"epoch_complete"|"…", "epoch":<int>, …}
        #   • Trimmed to the most recent `max_event_entries` events.
        self.event_stream = f"{prefix}event_stream"

        # Hash (Structure) of in‐progress epochs' resume positions:
        #   • Field = epoch_number (as string)
        #   • Value = last_processed_block_height (int)
        #   • Used so that send_batch can resume mid‐epoch on retry.
        self.resume_map = f"{prefix}resume_map"

        # Set of epochs that have been fully processed by a worker,
        # but are waiting for all earlier epochs to complete before
        # advancing `last_synced_epoch`.
        self.ready_set = f"{prefix}ready_set"

        # String value of the highest epoch N such that *all* epochs
        # from start through N have been successfully marked complete and synchronized.
        self.last_synced_epoch = f"{prefix}last_synced_epoch"

        self.start_epoch = start_epoch
        self.max_data_batches = max_data_batches
        self.max_event_entries = max_event_entries

        # Store settings for use in __aenter__
        if redis_settings is None:
            from config.settings import get_redis_settings
            redis_settings = get_redis_settings()
        self._redis_settings = redis_settings

        self.redis: aioredis.Redis | None = None  # type: ignore
        self._advance_sha: str | None = None
        self.logger = get_run_logger()
        self.backpressure_monitor: RedisBackpressureMonitor | None = None
        
        # Initialize backpressure config using Redis settings if not provided
        if backpressure_config is None:
            backpressure_config = RedisBackpressureConfig(
                max_depth=self._redis_settings.max_stream_depth,
                check_interval=self._redis_settings.check_interval,
            )
        
        self.backpressure_config = backpressure_config
        self.prefix = prefix

        # Stream of block‐batch payloads:
        #   • Entries: {"type":"batch", "epoch":<int>, "data":<orjson bytes>}
        #   • Trimmed to the most recent `max_data_batches` batches.
        self.data_stream = f"{prefix}data_stream"

        # Stream of audit/control events:
        #   • Entries: {"type":"batch_sent"|"epoch_complete"|"…", "epoch":<int>, …}
        #   • Trimmed to the most recent `max_event_entries` events.
        self.event_stream = f"{prefix}event_stream"

        # Hash (Structure) of in‐progress epochs’ resume positions:
        #   • Field = epoch_number (as string)
        #   • Value = last_processed_block_height (int)
        #   • Used so that send_batch can resume mid‐epoch on retry.
        self.resume_map = f"{prefix}resume_map"

        # Set of epochs that have been fully processed by a worker,
        # but are waiting for all earlier epochs to complete before
        # advancing `last_synced_epoch`.
        self.ready_set = f"{prefix}ready_set"

        # String value of the highest epoch N such that *all* epochs
        # from start through N have been successfully marked complete and synchronized.
        self.last_synced_epoch = f"{prefix}last_synced_epoch"

        self.start_epoch = start_epoch
        self.max_data_batches = max_data_batches
        self.max_event_entries = max_event_entries

        self.redis: aioredis.Redis | None = None  # type: ignore
        self._advance_sha: str | None = None
        self.logger = get_run_logger()
        self.backpressure_monitor: RedisBackpressureMonitor | None = None
        
        self.backpressure_config = backpressure_config

    async def __aenter__(self):
        url = self._redis_settings.url
        self.redis = aioredis.from_url(url, decode_responses=False)
        self.logger.debug(f"🔗 Connecting to Redis at {url}")

        # Initialize and start backpressure monitor
        self.backpressure_monitor = RedisBackpressureMonitor(
            redis_client=self.redis,
            stream_key=self.data_stream,
            config=self.backpressure_config,
        )
        self.backpressure_monitor.start()
        self.logger.debug("✅ Backpressure monitor started")

        # load our Lua once
        self._advance_sha = await self.redis.script_load(_ADVANCE_EPOCH_LUA)
        self.logger.debug("✅ loaded Lua advance script")
        # ensure last_synced_epoch exists
        await self.redis.setnx(self.last_synced_epoch, self.start_epoch - 1)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.backpressure_monitor:
            await self.backpressure_monitor.stop()
            self.logger.debug("🛑 Backpressure monitor stopped")
        if self.redis:
            await self.redis.close()
            self.logger.debug("🛑 Redis connection closed")

    async def send_batch(self, blocks: list[Block], **kwargs: Any) -> None:
        assert self.redis, "Not initialized"
        assert self.backpressure_monitor, "Backpressure monitor not initialized"

        # Wait if backpressure is active
        await self.backpressure_monitor.wait_if_paused()

        epoch = kwargs.pop("epoch")
        last_height = blocks[-1].height

        pipe = self.redis.pipeline(transaction=True)
        # 1) data stream
        batch_list = [await self._prepare_block(b) for b in blocks]
        payload = json.dumps(batch_list)
        pipe.xadd(
            self.data_stream,
            {"type": "batch", "epoch": epoch, "data": payload},
            maxlen=self.max_data_batches,
            approximate=True,
        )
        # 2) event stream
        pipe.xadd(
            self.event_stream,
            {"type": "batch_sent", "epoch": epoch, "height": last_height},
            maxlen=self.max_event_entries,
            approximate=True,
        )
        # 3) resume cursor
        pipe.hset(self.resume_map, epoch, last_height)
        await pipe.execute()

        self.logger.debug(f"Sent batch for epoch {epoch}, up to height {last_height}")

    async def mark_epoch_complete(
        self, epoch: EpochNumber, last_height: BlockHeight
    ) -> EpochNumber:
        """
        Marks an epoch as complete by updating Redis with the relevant information
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
            maxlen=self.max_event_entries,
            approximate=True,
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
            f"Epoch {epoch} marked complete; last_synced_epoch → {last_synced}"
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

    async def get_status(self) -> dict[str, Any]:
        assert self.redis, "Not initialized"
        return {
            "last_synced_epoch": await self.get_last_synced_epoch(),
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
