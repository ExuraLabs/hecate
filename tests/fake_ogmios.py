"""Chains the test owns, as blocks Ogmios v6 would render them.

Blocks are built with ``grow``, off a parent, labelled by fork so two forks
never share a hash.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import ogmios.response_handler as rh
from ogmios import Block, Point, Tip

# Builds blocks the way the backfill does, so the test's blocks match what it
# would construct from the same JSON.
import backfill  # noqa: F401


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
