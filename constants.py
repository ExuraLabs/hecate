import csv
from pathlib import Path

from models import BlockHash, EpochData, EpochNumber, Slot

BLOCKS_IN_EPOCH: dict[int, int]

with open(Path(__file__).parent / "data/epoch_blocks.csv", mode="r") as file:
    reader = csv.DictReader(file)
    BLOCKS_IN_EPOCH = {int(row["epoch"]): int(row["blocks"]) for row in reader}

EPOCH_BOUNDARIES: dict[EpochNumber, EpochData]

with open(Path(__file__).parent / "data/epoch_boundaries.csv", mode="r") as file:
    reader = csv.DictReader(file)
    parsed_epochs = [
        EpochData(
            **{  # type: ignore[arg-type]
                key: value if "hash" in key else int(value)
                for key, value in row.items()
            }
        )
        for row in reader
    ]
    EPOCH_BOUNDARIES = {epoch.number: epoch for epoch in parsed_epochs}

ERA_BOUNDARY = {  # Slot and hash of the last block of the era. Useful for testing.
    "byron": (
        Slot(4492799),
        BlockHash("f8084c61b6a238acec985b59310b6ecec49c0ab8352249afd7268da5cff2a457"),
    ),
    "shelley": (
        Slot(16588737),
        BlockHash("4e9bbbb67e3ae262133d94c3da5bffce7b1127fc436e7433b87668dba34c354a"),
    ),
    "allegra": (
        Slot(23068793),
        BlockHash("69c44ac1dda2ec74646e4223bc804d9126f719b1c245dadc2ad65e8de1b276d7"),
    ),
    "mary": (
        Slot(39916796),
        BlockHash("e72579ff89dc9ed325b723a33624b596c08141c7bd573ecfff56a1f7229e4d09"),
    ),
    "alonzo": (
        Slot(72316796),
        BlockHash("c58a24ba8203e7629422a24d9dc68ce2ed495420bf40d9dab124373655161a20"),
    ),
    "babbage": (
        Slot(133660799),
        BlockHash("e757d57eb8dc9500a61c60a39fadb63d9be6973ba96ae337fd24453d4d15c343"),
    ),
}
