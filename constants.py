import csv
from pathlib import Path

from models import EpochData, EpochNumber

BLOCKS_IN_EPOCH: dict[int, int]

with open(Path(__file__).parent / "data/epoch_blocks.csv", mode="r") as file:
    reader = csv.DictReader(file)
    BLOCKS_IN_EPOCH = {int(row["epoch"]): int(row["blocks"]) for row in reader}

EPOCH_BOUNDARIES: dict[EpochNumber, EpochData]

with open(Path(__file__).parent / "data/epoch_boundaries.csv", mode="r") as file:
    reader = csv.DictReader(file)
    parsed_epochs = [
        EpochData(**{  # type: ignore[arg-type]
            key: value if "hash" in key else int(value)
            for key, value in row.items()
        })
        for row in reader
    ]
    EPOCH_BOUNDARIES = {
        epoch.number: epoch for epoch in parsed_epochs
    }
