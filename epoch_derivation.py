"""Derive epoch boundary rows — per-epoch start/end block (height, slot, hash)
and block count — directly from an Ogmios chain-sync endpoint.

Uses only the standard Ogmios mini-protocols (``findIntersection`` +
``nextBlock``), so any cardano-node + Ogmios can serve it; a kupo endpoint, when
supplied, is an optional accelerator for locating epoch boundaries.

Post-Shelley epochs are a fixed number of slots long, so epoch ``e`` begins at
``SHELLEY_START_SLOT + (e - FIRST_SHELLEY_EPOCH) * SHELLEY_EPOCH_LENGTH``. A row
then needs only the last block before the next epoch's boundary slot and that
block's height; the block count is ``end_height - start_height + 1``.
"""

from __future__ import annotations

import requests
from ogmios import Block, Point

from client import HecateClient
from constants import FIRST_SHELLEY_EPOCH
from models import BlockHash, BlockHeight, EpochData, EpochNumber, Slot

# Slot of the first block-producing slot of the first Shelley epoch (epoch 208)
# and the constant Shelley epoch length in slots. Byron epochs are not covered
# by this arithmetic and are treated as fixed anchors (see constants.ERA_BOUNDARY).
SHELLEY_START_SLOT = 4_492_800
SHELLEY_EPOCH_LENGTH = 432_000

KUPO_TIMEOUT_SECONDS = 30


def epoch_start_slot(epoch: EpochNumber) -> Slot:
    """First slot of ``epoch`` (valid for Shelley and every later era)."""
    return Slot(
        SHELLEY_START_SLOT + (epoch - FIRST_SHELLEY_EPOCH) * SHELLEY_EPOCH_LENGTH
    )


async def peek_first_block_after(client: HecateClient, point: Point) -> Block:
    """Return the first block strictly after ``point`` on the chain.

    Intersects at ``point`` (which must be on-chain) and reads one block forward.
    The intersection is verified, so a wrong or rolled-back point fails loudly
    rather than silently returning data from a different chain position.

    Note: if ``point`` is the chain tip there is no next block yet and this will
    time out — callers must only pass finalized points.
    """
    intersection, _tip, _ = await client.find_intersection.execute(points=[point])
    if intersection != point:
        raise ValueError(
            f"Point {point} is not on-chain; intersected {intersection} instead"
        )
    # batched(2) sends two nextBlock requests: the first response is the roll-back
    # to the intersection (a Point, filtered out), the second is the first block.
    blocks = await client.next_block.batched(batch_size=2)
    if not blocks:
        raise ValueError(f"No block found after {point}; is it the chain tip?")
    return blocks[0]


def kupo_end_point(kupo_url: str, epoch: EpochNumber) -> Point:
    """Last block at or before the final slot of ``epoch``, via kupo (one call).

    Queries the closest-ancestor checkpoint for the last slot that still belongs
    to ``epoch`` (``epoch_start_slot(epoch + 1) - 1``).
    """
    last_slot = epoch_start_slot(EpochNumber(epoch + 1)) - 1
    resp = requests.get(
        f"{kupo_url.rstrip('/')}/checkpoints/{last_slot}", timeout=KUPO_TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    data = resp.json()
    return Point(slot=Slot(int(data["slot_no"])), id=BlockHash(data["header_hash"]))


def _assert_astride(epoch: EpochNumber, end_slot: Slot, next_first: Block) -> None:
    """Verify the boundary between ``epoch`` and ``epoch + 1`` sits on its slot line."""
    boundary = epoch_start_slot(EpochNumber(epoch + 1))
    if not (end_slot < boundary <= next_first.slot):
        raise ValueError(
            f"Epoch {epoch} boundary not astride slot {boundary}: "
            f"end_slot={end_slot}, next_first_slot={next_first.slot} "
            f"(epoch may not be finalized, or boundary data disagrees)"
        )


async def walk_boundary(
    client: HecateClient, epoch: EpochNumber, start_block: Block
) -> tuple[Slot, BlockHash, Block]:
    """Find ``epoch``'s end boundary by reading forward over chain-sync (no kupo).

    Walks from ``start_block`` (the first block of ``epoch``) until it reads a
    block in ``epoch + 1``. Returns ``(end_slot, end_hash, next_first)`` where the
    first two identify the last block of ``epoch`` and ``next_first`` is the first
    block of ``epoch + 1``. Relies only on standard Ogmios chain-sync, so it works
    against any Ogmios endpoint; it reads a whole epoch of blocks, so it is the
    slower path used when no kupo accelerator is configured.
    """
    boundary = epoch_start_slot(EpochNumber(epoch + 1))
    start_point = Point(slot=Slot(start_block.slot), id=BlockHash(start_block.id))
    intersection, _tip, _ = await client.find_intersection.execute(points=[start_point])
    if intersection != start_point:
        raise ValueError(f"Start block of epoch {epoch} not on-chain: {intersection}")

    last = start_block
    while True:
        batch = await client.next_block.batched()
        if not batch:
            raise ValueError(
                f"Chain-sync exhausted before crossing epoch {epoch} boundary {boundary}"
            )
        for block in batch:
            if block.slot >= boundary:
                return Slot(last.slot), BlockHash(last.id), block
            last = block


async def _resolve_boundary(
    client: HecateClient,
    epoch: EpochNumber,
    start_block: Block,
    kupo_url: str | None,
) -> tuple[Slot, BlockHash, Block]:
    """Return ``(end_slot, end_hash, next_first)`` for ``epoch``'s end boundary.

    Uses kupo to jump to the boundary when ``kupo_url`` is given (verified astride
    its theoretical slot, so a stale/pruned kupo answer raises rather than yielding
    a wrong boundary); otherwise walks the epoch over pure chain-sync.
    """
    if kupo_url is not None:
        end_point = kupo_end_point(kupo_url, epoch)
        next_first = await peek_first_block_after(client, end_point)
        _assert_astride(epoch, end_point.slot, next_first)
        return Slot(end_point.slot), BlockHash(end_point.id), next_first
    return await walk_boundary(client, epoch, start_block)


async def derive_epoch(
    client: HecateClient,
    epoch: EpochNumber,
    previous: EpochData,
    *,
    kupo_url: str | None = None,
) -> tuple[EpochData, int]:
    """Derive full ``EpochData`` and block count for a finalized ``epoch``.

    ``previous`` is the known-correct ``EpochData`` for ``epoch - 1``, used as the
    chain-sync anchor. ``epoch`` must be finalized — its successor must have
    started — otherwise the boundary has no following block to read. ``kupo_url``
    is an optional accelerator; when ``None`` the boundary is found by a pure
    chain-sync walk.

    Two independent cross-checks fail loudly on any disagreement: the derived
    first block must be contiguous with ``previous`` (height ``+1``), and the
    boundary must be astride its theoretical slot.
    """
    start_block = await peek_first_block_after(
        client, Point(slot=previous.end_slot, id=previous.end_hash)
    )
    if start_block.height != previous.end_height + 1:
        raise ValueError(
            f"Epoch {epoch} start height {start_block.height} is not contiguous with "
            f"epoch {epoch - 1} end height {previous.end_height}"
        )

    end_slot, end_hash, next_first = await _resolve_boundary(
        client, epoch, start_block, kupo_url
    )

    end_height = BlockHeight(next_first.height - 1)
    row = EpochData(
        number=epoch,
        start_slot=Slot(start_block.slot),
        end_slot=end_slot,
        start_height=BlockHeight(start_block.height),
        end_height=end_height,
        start_hash=BlockHash(start_block.id),
        end_hash=end_hash,
    )
    return row, end_height - start_block.height + 1


async def regenerate_range(
    client: HecateClient,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber,
    anchor: EpochData,
    *,
    kupo_url: str | None = None,
) -> tuple[dict[EpochNumber, EpochData], dict[EpochNumber, int]]:
    """Regenerate rows for ``[start_epoch, end_epoch]`` straight from the chain.

    ``anchor`` is the known-correct ``EpochData`` for ``start_epoch - 1``. Each
    epoch's end boundary yields the *first block of the next epoch*, carried
    forward as the next epoch's start block. ``kupo_url`` is an optional
    accelerator; when ``None`` every boundary is found by a pure chain-sync walk
    (reads the full chain over the range, so far slower). Every boundary is checked
    astride its theoretical slot; disagreements raise.
    """
    rows: dict[EpochNumber, EpochData] = {}
    counts: dict[EpochNumber, int] = {}

    cur_start = await peek_first_block_after(
        client, Point(slot=anchor.end_slot, id=anchor.end_hash)
    )
    if cur_start.height != anchor.end_height + 1:
        raise ValueError(
            f"Anchor discontinuity: epoch {start_epoch} start height "
            f"{cur_start.height} != anchor end height {anchor.end_height} + 1"
        )

    for e in range(start_epoch, end_epoch + 1):
        epoch = EpochNumber(e)
        end_slot, end_hash, next_first = await _resolve_boundary(
            client, epoch, cur_start, kupo_url
        )

        end_height = BlockHeight(next_first.height - 1)
        rows[epoch] = EpochData(
            number=epoch,
            start_slot=Slot(cur_start.slot),
            end_slot=end_slot,
            start_height=BlockHeight(cur_start.height),
            end_height=end_height,
            start_hash=BlockHash(cur_start.id),
            end_hash=end_hash,
        )
        counts[epoch] = end_height - cur_start.height + 1
        cur_start = next_first  # first block of epoch+1 anchors the next iteration

    return rows, counts


def verify_rows(
    rows: dict[EpochNumber, EpochData],
    counts: dict[EpochNumber, int],
) -> list[str]:
    """Check a table of epoch rows against chain-intrinsic invariants.

    Returns a list of human-readable problems (empty ⇒ the table is internally
    consistent). Checks, per epoch, that: the recorded block count equals
    ``end_height - start_height + 1``; heights are contiguous with the previous
    row; the end block falls before the next epoch's boundary slot; and the next
    row's start block falls on/after that boundary (astride). None of these
    reference any external source.
    """
    problems: list[str] = []
    for epoch in sorted(rows):
        row = rows[epoch]
        expected_count = row.end_height - row.start_height + 1
        if counts.get(epoch) != expected_count:
            problems.append(
                f"epoch {epoch}: block count {counts.get(epoch)} != "
                f"end-start+1 {expected_count}"
            )

        previous = rows.get(EpochNumber(epoch - 1))
        if previous is not None and row.start_height != previous.end_height + 1:
            problems.append(
                f"epoch {epoch}: start_height {row.start_height} not contiguous with "
                f"epoch {epoch - 1} end_height {previous.end_height}"
            )

        boundary = epoch_start_slot(EpochNumber(epoch + 1))
        if row.end_slot >= boundary:
            problems.append(
                f"epoch {epoch}: end_slot {row.end_slot} not before boundary {boundary}"
            )
        following = rows.get(EpochNumber(epoch + 1))
        if following is not None and following.start_slot < boundary:
            problems.append(
                f"epoch {epoch}: next start_slot {following.start_slot} before boundary {boundary}"
            )

    return problems
