import csv
import logging
import os
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import Any

import json
import requests
from prefect import task, flow

from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES
from models import BlockHash, BlockHeight, EpochData, EpochNumber, Slot


@task
def get_epoch_boundaries(epoch: EpochNumber) -> EpochData:
    """
    Gets the `EpochData` of a given epoch from constants if the values are known;
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

    if epoch not in BLOCKS_IN_EPOCH:
        raise ValueError(
            f"Produced blocks for Epoch {epoch} is not available."
            "Make sure get_epoch_blocks() has been called first."
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

    start_height = BlockHeight(previous_boundaries.end_height + 1)
    response = requests.get(f"{base_url}{start_height}")
    start_slot, start_hash = parse_response(response, start_height)

    end_height = BlockHeight(start_height + BLOCKS_IN_EPOCH[epoch] - 1)
    response = requests.get(f"{base_url}{end_height}")
    end_slot, end_hash = parse_response(response, end_height)

    result = EpochData(
        number=epoch,
        start_slot=start_slot,
        end_slot=end_slot,
        start_height=start_height,
        end_height=end_height,
        start_hash=start_hash,
        end_hash=end_hash,
    )
    EPOCH_BOUNDARIES[epoch] = result
    return result


@task
def get_epoch_blocks(epoch: EpochNumber) -> int:
    """
    Get the number of blocks produced in an epoch from constants if the values are known;
    Otherwise obtained from the Koios API as the sum of blocks produced by each protocol version.
    If the value is obtained from the Koios API, it is cached in the constants for future use.
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
    result = parse_response(response)
    BLOCKS_IN_EPOCH[epoch] = result
    return result


def get_current_epoch() -> EpochNumber:
    """
    Get the current epoch from the Koios API.
    """

    def parse_response(_response: Any) -> EpochNumber:
        """
        Parse the response from the Koios API to get the current epoch.
        """
        if _response.ok:
            data = _response.json()
            return EpochNumber(data[0]["epoch_no"])
        raise Exception(
            f"Error parsing response for current epoch: "
            f"({_response.status_code}) - {_response.text}"
        )

    base_url = "https://api.koios.rest/api/v1/tip"
    response = requests.get(base_url)
    return parse_response(response)


def get_system_epoch() -> EpochNumber:
    """
    Get the last processed epoch from the checkpoint file.
    """
    with open(Path(__file__).parent / "checkpoint.json", "r") as file:
        checkpoint = json.loads(file.read())
        return EpochNumber(checkpoint["epoch"])


def update_checkpoint(epoch: EpochNumber) -> None:
    """
    Updates the epoch data and checkpoint files with the latest processed epoch.
    """
    logging.info(f"Fetching data for epoch {epoch}...")
    checkpoint = {"epoch": epoch}
    block_count = get_epoch_blocks(epoch)
    boundaries = get_epoch_boundaries(epoch)
    data_dir = Path(__file__).parent.parent / "data"

    with open(data_dir / "epoch_boundaries.csv", "a") as boundaries_file:
        epoch_data = asdict(boundaries)
        fields = [
            "number",
            "start_height",
            "start_slot",
            "start_hash",
            "end_height",
            "end_slot",
            "end_hash",
        ]
        dict_writer = csv.DictWriter(boundaries_file, fieldnames=fields)
        dict_writer.writerow(epoch_data)

    with open(data_dir / "epoch_blocks.csv", "a") as blocks_file:
        writer = csv.writer(blocks_file)
        writer.writerow([epoch, block_count])

    with open(Path(__file__).parent / "checkpoint.json", "w") as checkpoint_file:
        checkpoint_file.write(json.dumps(checkpoint, indent=2))
    logging.info(f"Checkpoint updated to epoch {epoch}.")


@task
def commit_and_push_changes(epoch: EpochNumber) -> None:
    """
    Stage any changes to flows/checkpoint.json and data/,
    commit them if there’s anything new, and push to
    the automated/epoch-updates branch on GitHub.
    This function uses the GitHub token from the GITHUB_TOKEN environment variable.
    """
    _BASE_DIR = Path(__file__).parent.parent
    _GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    if not _GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN environment variable is not set.")

    # Stage files
    subprocess.run(
        ["git", "add", "flows/checkpoint.json", "data"],
        cwd=_BASE_DIR,
        check=True,
    )

    # If nothing to commit, exit
    if subprocess.call(["git", "diff", "--cached", "--quiet"], cwd=_BASE_DIR) == 0:
        logging.info("No new changes to commit.")
        return

    commit_msg = f"Automated epoch data update for epoch {epoch}"
    subprocess.run(["git", "commit", "-m", commit_msg], cwd=_BASE_DIR, check=True)

    # Push back to GitHub over HTTPS (token in-line)
    remote_url = (
        f"https://x-access-token:{_GITHUB_TOKEN}@github.com/ExuraLabs/hecate.git"
    )
    subprocess.run(
        ["git", "push", "--force", remote_url, "HEAD:automated/epoch-updates"],
        cwd=_BASE_DIR,
        check=True,
    )
    logging.info("Changes pushed to automated/epoch-updates.")


@flow(name="Fetch Epoch Data")
def fetch_epoch_data_flow() -> EpochNumber | None:
    """
    Fetches, processes, and updates the system's epoch data. This flow determines the
    next epoch number based on the system's current checkpoint and updates it if necessary.
    If the current checkpoint is found to already be up to date with the current epoch,
    no actions are performed. Otherwise, the new checkpoint is committed and pushed.

    :return: The updated upcoming epoch number if changes were made, otherwise None.
    :rtype: EpochNumber | None
    """

    checkpoint = get_system_epoch()
    current_epoch = get_current_epoch()
    upcoming = EpochNumber(checkpoint + 1)

    if upcoming == current_epoch:
        logging.info(
            f"Checkpoint epoch {checkpoint} is already up-to-date; nothing to do."
        )
        return None

    logging.info(f"Current epoch is {current_epoch}")

    update_checkpoint(upcoming)
    commit_and_push_changes(upcoming)
    return upcoming
