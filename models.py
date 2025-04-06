from dataclasses import dataclass
from typing import NewType

Slot = NewType("Slot", int)
BlockHeight = NewType("BlockHeight", int)
EpochNumber = NewType("EpochNumber", int)
BlockHash = NewType("BlockHash", str)


@dataclass(frozen=True)
class EpochData:
    """Represents key information about an epoch"""
    number: EpochNumber
    start_slot: Slot
    end_slot: Slot
    start_height: BlockHeight
    end_height: BlockHeight
    start_hash: BlockHash
    end_hash: BlockHash
