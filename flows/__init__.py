import json
from pathlib import Path

from models import EpochNumber


def get_system_checkpoint() -> EpochNumber:
    """
    Get the current system checkpoint from the checkpoint file.
    """
    with open(Path(__file__).parent / "checkpoint.json", "r") as file:
        checkpoint = json.loads(file.read())
        return EpochNumber(checkpoint["epoch"])
