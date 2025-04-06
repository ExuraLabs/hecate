from typing import Any

import requests

from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES
from models import BlockHash, BlockHeight, EpochData, EpochNumber, Slot


def get_epoch_boundaries(epoch: EpochNumber) -> EpochData:
    """
    Get the start and end slots of an epoch from constants if the values are known;
    grabbed from AdaStat API otherwise.
    Requires knowing the slot boundaries of the previous epoch AND
    the number of blocks produced in the given epoch. (Both read from CSV into client.constants)
    :param epoch:
    :return:
    """
    if epoch in EPOCH_BOUNDARIES:
        return EPOCH_BOUNDARIES[epoch]

    if (previous_epoch := EpochNumber(epoch - 1)) not in EPOCH_BOUNDARIES:
        raise ValueError(
            f"Epoch {epoch} is not valid. "
            f"Please check the epoch boundaries."
        )

    def parse_response(_response: Any, queried_height: BlockHeight) -> tuple[Slot, BlockHash]:
        """
        Parse the response from the AdaStat API to get the slot and hash for the queried height.
        """
        if _response.ok:
            data = _response.json()
            for block in data.get("blocks", []):
                if block["no"] == queried_height:
                    return Slot(int(block["slot_no"])), BlockHash(block["hash"])
        raise Exception(
            f"Error parsing response for height {queried_height}: "
            f"({_response.status_code}) - {_response.text}"
        )

    base_url = "https://adastat.net/api/rest/v1/search.json?query="
    previous_boundaries = EPOCH_BOUNDARIES[previous_epoch]

    start_height = BlockHeight(previous_boundaries.end_slot + 1)
    response = requests.get(f"{base_url}{start_height}")
    start_slot, start_hash = parse_response(response, start_height)

    end_height = BlockHeight(start_height + BLOCKS_IN_EPOCH[epoch] - 1)
    response = requests.get(f"{base_url}{end_height}")
    end_slot, end_hash = parse_response(response, end_height)

    return EpochData(
        number=epoch,
        start_slot=start_slot,
        end_slot=end_slot,
        start_height=start_height,
        end_height=end_height,
        start_hash=start_hash,
        end_hash=end_hash,
    )
