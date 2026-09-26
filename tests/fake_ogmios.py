"""A stand-in Ogmios: chain-sync over a websocket, against a chain the test owns.

It answers ``findIntersection`` and ``nextBlock`` with hand-written Ogmios v6
JSON-RPC replies, and models the one thing about chain-sync that matters to a
follower: the node keeps a cursor per connection, and when its chain stops
containing that cursor it rolls the client back to the newest point they still
share. So a test swaps the node's chain for a fork (``set_chain``) and the
follower sees exactly the ``RollBackward`` / ``RollForward`` sequence a real
node would send, including requests held at the tip until there is something
to say.

Blocks are built with ``grow``, off a parent, labelled by fork so two forks
never share a hash.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import Any

import orjson
import ogmios.response_handler as rh
from ogmios import Block, Point, Tip
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

# Builds blocks the way the relay does, so the test's blocks match what the
# live path would construct from the same JSON.
import backfill  # noqa: F401

#: Ogmios v6's code for "none of those points are on my chain".
INTERSECTION_NOT_FOUND = 1000


def block_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class FakeBlock:
    slot: int
    hash: str
    height: int
    ancestor: str

    @property
    def point(self) -> Point:
        return Point(slot=self.slot, id=self.hash)

    @property
    def tip(self) -> Tip:
        return Tip(slot=self.slot, id=self.hash, height=self.height)

    def as_json(self) -> dict[str, Any]:
        """A praos block as Ogmios v6 renders one, with one transaction."""
        return {
            "type": "praos",
            "era": "conway",
            "id": self.hash,
            "ancestor": self.ancestor,
            "height": self.height,
            "slot": self.slot,
            "size": {"bytes": 1024},
            "protocol": {"version": {"major": 10, "minor": 2, "patch": 0}},
            "issuer": {
                "verificationKey": "ab" * 32,
                "vrfVerificationKey": "cd" * 32,
                "operationalCertificate": {
                    "count": 1,
                    "kes": {"period": 1, "verificationKey": "ef" * 32},
                },
                "leaderValue": {"proof": "00" * 80, "output": "00" * 64},
            },
            "transactions": [
                {
                    "id": block_hash(f"tx:{self.hash}"),
                    "spends": "inputs",
                    "inputs": [],
                    "outputs": [],
                    "fee": {"ada": {"lovelace": 170_000}},
                    "datums": {"ff" * 32: "d87980"},
                }
            ],
        }

    def as_block(self) -> Block:
        return rh.parse_Block(self.as_json())


#: Where every test chain starts: an arbitrary Conway-era block.
ANCHOR = FakeBlock(
    slot=150_000_000,
    hash=block_hash("anchor"),
    height=11_000_000,
    ancestor=block_hash("before the anchor"),
)


def grow(
    parent: FakeBlock, count: int, *, fork: str = "main", slot_step: int = 20
) -> list[FakeBlock]:
    """``count`` blocks, each built on the one before, starting on ``parent``."""
    blocks = []
    for _ in range(count):
        parent = FakeBlock(
            slot=parent.slot + slot_step,
            hash=block_hash(f"{fork}:{parent.height + 1}"),
            height=parent.height + 1,
            ancestor=parent.hash,
        )
        blocks.append(parent)
    return blocks


def _point_json(block: FakeBlock) -> dict[str, Any]:
    return {"slot": block.slot, "id": block.hash}


def _tip_json(block: FakeBlock) -> dict[str, Any]:
    return {"slot": block.slot, "id": block.hash, "height": block.height}


@dataclass
class _Session:
    """One client connection's chain-sync cursor, and what it has been sent."""

    cursor: FakeBlock | None = None
    sent: list[FakeBlock] = field(default_factory=list)
    announce_intersection: bool = False
    repeated_tip: bool = False


class FakeOgmios:
    """Serves ``chain`` (oldest first) until a test replaces it."""

    def __init__(
        self,
        chain: list[FakeBlock],
        *,
        repeat_tip: bool = False,
        omit_rollbacks: bool = False,
    ):
        self.chain = list(chain)
        #: Behave as dolos does on a fork switch: go straight on to the new
        #: fork's blocks without rolling back to where it leaves the old one.
        self.omit_rollbacks = omit_rollbacks
        #: Behave as dolos does: the first time a session sits at the tip, roll
        #: forward to the tip block once more.
        self.repeat_tip = repeat_tip
        self.tip_repeats = 0
        self.url = ""
        #: Every findIntersection request, as the (slot, hash) points offered.
        self.intersection_requests: list[list[tuple[int, str]]] = []
        self.connections_served = 0
        self._changed: asyncio.Condition | None = None
        self._server: Server | None = None
        self._connections: set[ServerConnection] = set()

    async def __aenter__(self) -> FakeOgmios:
        self._changed = asyncio.Condition()
        self._server = await serve(self._handle, "127.0.0.1", 0)
        port = next(iter(self._server.sockets)).getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def set_chain(self, chain: list[FakeBlock]) -> None:
        """Switch the node to ``chain``: an extension, or a fork of the old one."""
        assert self._changed is not None
        async with self._changed:
            self.chain = list(chain)
            self._changed.notify_all()

    async def drop_connections(self) -> None:
        for connection in list(self._connections):
            await connection.close()

    def _position(self, block: FakeBlock) -> int | None:
        for index, candidate in enumerate(self.chain):
            if candidate.hash == block.hash:
                return index
        return None

    async def _handle(self, connection: ServerConnection) -> None:
        self._connections.add(connection)
        self.connections_served += 1
        session = _Session()
        pending: asyncio.Queue[Any] = asyncio.Queue()
        responder = asyncio.create_task(self._respond(connection, session, pending))
        try:
            async for message in connection:
                request = orjson.loads(message)
                method = request["method"]
                if method == "findIntersection":
                    reply = self._find_intersection(session, request)
                    await connection.send(orjson.dumps(reply).decode())
                elif method == "nextBlock":
                    await pending.put(request.get("id"))
                else:
                    raise AssertionError(f"the fake does not serve {method}")
        except ConnectionClosed:
            pass
        finally:
            responder.cancel()
            self._connections.discard(connection)

    def _find_intersection(
        self, session: _Session, request: dict[str, Any]
    ) -> dict[str, Any]:
        points = request["params"]["points"]
        self.intersection_requests.append([(p["slot"], p["id"]) for p in points])
        tip = _tip_json(self.chain[-1])
        for point in points:
            for block in self.chain:
                if block.hash == point["id"] and block.slot == point["slot"]:
                    session.cursor = block
                    session.sent = [block]
                    session.announce_intersection = True
                    return {
                        "jsonrpc": "2.0",
                        "method": "findIntersection",
                        "result": {"intersection": _point_json(block), "tip": tip},
                        "id": request.get("id"),
                    }
        return {
            "jsonrpc": "2.0",
            "method": "findIntersection",
            "error": {
                "code": INTERSECTION_NOT_FOUND,
                "message": "No intersection found.",
                "data": {"tip": tip},
            },
            "id": request.get("id"),
        }

    async def _respond(
        self,
        connection: ServerConnection,
        session: _Session,
        pending: asyncio.Queue[Any],
    ) -> None:
        while True:
            request_id = await pending.get()
            reply = {
                "jsonrpc": "2.0",
                "method": "nextBlock",
                "result": await self._next(session),
                "id": request_id,
            }
            try:
                await connection.send(orjson.dumps(reply).decode())
            except ConnectionClosed:
                return

    async def _next(self, session: _Session) -> dict[str, Any]:
        """The reply the node owes this session next, waiting at the tip."""
        assert self._changed is not None
        async with self._changed:
            while True:
                tip = _tip_json(self.chain[-1])
                cursor = session.cursor
                assert cursor is not None, "nextBlock before findIntersection"

                if session.announce_intersection:
                    session.announce_intersection = False
                    return self._backward(cursor, tip)

                position = self._position(cursor)
                if position is None:
                    for index in range(len(session.sent) - 1, -1, -1):
                        shared = session.sent[index]
                        if self._position(shared) is not None:
                            session.cursor = shared
                            del session.sent[index + 1 :]
                            if self.omit_rollbacks:
                                break
                            return self._backward(shared, tip)
                    else:
                        raise AssertionError("the client shares no point with the node")
                    continue

                at_tip = position == len(self.chain) - 1
                if self.repeat_tip and at_tip and not session.repeated_tip:
                    session.repeated_tip = True
                    self.tip_repeats += 1
                    return {
                        "direction": "forward",
                        "block": cursor.as_json(),
                        "tip": tip,
                    }

                if position + 1 < len(self.chain):
                    block = self.chain[position + 1]
                    session.cursor = block
                    session.sent.append(block)
                    return {
                        "direction": "forward",
                        "block": block.as_json(),
                        "tip": tip,
                    }

                await self._changed.wait()

    @staticmethod
    def _backward(point: FakeBlock, tip: dict[str, Any]) -> dict[str, Any]:
        return {"direction": "backward", "point": _point_json(point), "tip": tip}
