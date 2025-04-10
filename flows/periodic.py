from typing import Any

import requests
from prefect import task, flow
from prefect.runner.storage import GitRepository

from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES
from models import BlockHash, BlockHeight, EpochData, EpochNumber, Slot


@task
def get_epoch_boundaries(epoch: EpochNumber) -> EpochData:
    """
    Get the `EpochData` of a given epoch from constants if the values are known;
    queries them from AdaStat API otherwise.
    Requires knowing the slot boundaries of the previous epoch AND
    the number of blocks produced in the given epoch. (Both read from CSV into client.constants)
    :param epoch:
    :return:
    """
    if epoch in EPOCH_BOUNDARIES:
        return EPOCH_BOUNDARIES[epoch]

    if (previous_epoch := EpochNumber(epoch - 1)) not in EPOCH_BOUNDARIES:
        raise ValueError(
            f"Epoch {epoch} is not valid. Please check the epoch boundaries."
        )

    def parse_response(
        _response: Any, queried_height: BlockHeight
    ) -> tuple[Slot, BlockHash]:
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


@task
def get_epoch_blocks(epoch: EpochNumber) -> int:
    """
    Get the number of blocks produced in an epoch from constants if the values are known;
    Otherwise obtained from the Koios API as the sum of blocks produced by each protocol version.
    """

    if epoch in BLOCKS_IN_EPOCH:
        return BLOCKS_IN_EPOCH[epoch]

    def parse_response(_response: Any) -> int:
        """
        Parse the response from the Koios API to get the number of blocks in an epoch.
        """
        if _response.ok:
            data = _response.json()
            return sum(int(block["blocks"]) for block in data)
        raise Exception(
            f"Error parsing response for epoch {epoch}: "
            f"({_response.status_code}) - {_response.text}"
        )

    base_url = "https://api.koios.rest/api/v1/epoch_block_protocols?_epoch_no="
    response = requests.get(f"{base_url}{epoch}")
    return parse_response(response)


@flow(name="Fetch Epoch Data")
def fetch_epoch_data_flow(epoch: EpochNumber) -> tuple[EpochData, int]:
    """Flow that fetches all data for a specific epoch"""
    boundaries = get_epoch_boundaries(epoch)
    block_count = get_epoch_blocks(epoch)
    return boundaries, block_count


if __name__ == "__main__":
    git_repo = GitRepository(
        url="https://github.com/exuralabs/hecate.git",
        branch="create_prefect_flows",
    )

    # Deploy with daily schedule (midnight UTC)
    fetch_epoch_data_flow.from_source(  # type: ignore
        git_repo,
        entrypoint="flows.periodic:fetch_epoch_data_flow",
    ).deploy(
        name="daily-epoch-data-fetch",
        work_pool_name="exura-work-pool",
        cron="0 0 * * *",  # Run daily at midnight
        parameters={"epoch": 549},  # Starting epoch - cast to EpochNumber in the flow
    )
