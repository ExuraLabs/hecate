import json
from typing import Any

import redis.asyncio as redis
from ogmios import Block

from sinks.base import DataSink


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
        self.redis = redis.Redis(host=host, port=port, db=db, **redis_kwargs)
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

    async def send_batch(self, blocks: list[Block]) -> None:
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

    async def _prepare_block(self, block: Block) -> dict[str, Any]:
        """
        Prepare a block for sending to Redis.
        This method sends over the entire content of the block in dictionary format,
        minus the schema attribute.
        """
        block_data = {"slot": -1, "hash": ""}  # We want these fields to be first
        block_data |= vars(block)
        block_data.pop("_schematype")

        # Only opinionated modification we do:
        block_data["hash"] = block_data["id"]
        del block_data["id"]

        return block_data
