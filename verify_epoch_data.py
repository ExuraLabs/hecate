"""Verify and repair the committed epoch-data CSVs against the live chain.

    uv run python -m verify_epoch_data --verify              # read-only proof + diff
    uv run python -m verify_epoch_data --repair 643          # rewrite one row in place

``--verify`` regenerates every epoch from the chain (via an Ogmios endpoint, with
kupo as a boundary accelerator), checks the chain-intrinsic invariants
(contiguity + astride, see ``epoch_derivation.verify_rows``), and diffs the result
against ``data/epoch_boundaries.csv`` / ``data/epoch_blocks.csv``. ``--repair``
regenerates the named epoch(s), verifies them against their neighbours, and
rewrites just those rows, leaving every other line untouched (minimal diff).

The Ogmios endpoint comes from ``--ogmios`` / ``$OGMIOS_ENDPOINT`` and otherwise
the first configured Ogmios endpoint; kupo is optional (``--kupo`` / ``$KUPO_URL``)
and, when omitted, boundaries are found by a pure chain-sync walk. No infra
hostnames are hardcoded.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import asdict
from pathlib import Path

from client import HecateClient
from config.settings import ogmios_settings
from constants import BLOCKS_IN_EPOCH, EPOCH_BOUNDARIES, FIRST_SHELLEY_EPOCH
from epoch_derivation import regenerate_range, verify_rows
from models import EpochData, EpochNumber

DATA_DIR = Path(__file__).parent / "data"
BOUNDARIES_CSV = DATA_DIR / "epoch_boundaries.csv"
BLOCKS_CSV = DATA_DIR / "epoch_blocks.csv"

# Endpoints come from the environment / configured Ogmios settings — no infra
# hostnames are baked in. Ogmios falls back to the first configured endpoint;
# kupo is optional (unset ⇒ pure chain-sync walk, slower but Ogmios-only).
DEFAULT_OGMIOS = os.getenv("OGMIOS_ENDPOINT") or ogmios_settings.endpoints[0]
DEFAULT_KUPO = os.getenv("KUPO_URL")

_BOUNDARY_FIELDS = [
    "number",
    "start_height",
    "start_slot",
    "start_hash",
    "end_height",
    "end_slot",
    "end_hash",
]


def _boundary_line(row: EpochData) -> str:
    d = asdict(row)
    return ",".join(str(d[f]) for f in _BOUNDARY_FIELDS)


def _diff_row(epoch: EpochNumber, derived: EpochData, count: int) -> list[str]:
    """Field-level differences between a derived row and the committed CSV row."""
    diffs: list[str] = []
    committed = EPOCH_BOUNDARIES.get(epoch)
    if committed is None:
        diffs.append("absent from epoch_boundaries.csv")
    else:
        for field, got in asdict(derived).items():
            exp = getattr(committed, field)
            if got != exp:
                diffs.append(f"{field}: chain={got} csv={exp}")
    if BLOCKS_IN_EPOCH.get(epoch) != count:
        diffs.append(f"blocks: chain={count} csv={BLOCKS_IN_EPOCH.get(epoch)}")
    return diffs


def _source(ogmios: str, kupo: str | None) -> str:
    return (
        f"{ogmios} (kupo {kupo})"
        if kupo
        else f"{ogmios} (pure chain-sync walk, no kupo)"
    )


async def _regenerate_all(
    ogmios: str, kupo: str | None, start: EpochNumber, end: EpochNumber
) -> tuple[dict[EpochNumber, EpochData], dict[EpochNumber, int]]:
    anchor = EPOCH_BOUNDARIES[EpochNumber(start - 1)]
    async with HecateClient(endpoint_url=ogmios) as client:
        return await regenerate_range(client, start, end, anchor, kupo_url=kupo)


async def verify(ogmios: str, kupo: str | None) -> int:
    start = FIRST_SHELLEY_EPOCH
    end = max(EPOCH_BOUNDARIES)
    print(f"Regenerating epochs {start}..{end} from {_source(ogmios, kupo)} ...")
    rows, counts = await _regenerate_all(ogmios, kupo, start, end)

    problems = verify_rows(rows, counts)
    print(f"Chain-intrinsic invariants: {'PASS' if not problems else 'FAIL'}")
    for p in problems:
        print(f"  ! {p}")

    mismatches = {e: d for e in rows if (d := _diff_row(e, rows[e], counts[e]))}
    identical = len(rows) - len(mismatches)
    print(f"Diff vs CSV: {identical} identical, {len(mismatches)} differ")
    for e in sorted(mismatches):
        print(f"  epoch {e}: {'; '.join(mismatches[e])}")

    ok = not problems and not mismatches
    print(
        "\nRESULT: CSVs match the chain and are internally consistent."
        if ok
        else "\nRESULT: discrepancies found (see above)."
    )
    return 0 if ok else 1


def _rewrite_line(path: Path, key: int, new_line: str) -> bool:
    """Replace the CSV data line whose first column == ``key``. Returns True if changed.

    Preserves every other line byte-for-byte, including its terminator — the CSVs
    carry mixed LF/CRLF endings (the automated flow's ``csv.writer`` emits CRLF),
    so we keep the target row's original terminator and never touch its neighbours.
    """
    # newline="" disables universal-newline translation, so CRLF rows keep their
    # exact bytes and only the target line is ever rewritten.
    with open(path, "r", newline="") as f:
        lines = f.read().splitlines(keepends=True)
    prefix = f"{key},"
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            terminator = line[len(line.rstrip("\r\n")) :]
            replacement = new_line + terminator
            if line == replacement:
                return False
            lines[i] = replacement
            with open(path, "w", newline="") as f:
                f.write("".join(lines))
            return True
    raise ValueError(f"No row for {key} in {path.name}")


async def repair(ogmios: str, kupo: str | None, epochs: list[int]) -> int:
    targets = sorted(EpochNumber(e) for e in epochs)
    start, end = targets[0], targets[-1]
    print(
        f"Regenerating epochs {start}..{end} from {_source(ogmios, kupo)} for repair ..."
    )
    rows, counts = await _regenerate_all(ogmios, kupo, start, end)

    # Verify the repaired rows together with their committed neighbours.
    context = dict(EPOCH_BOUNDARIES)
    context_counts: dict[EpochNumber, int] = {
        EpochNumber(e): c for e, c in BLOCKS_IN_EPOCH.items()
    }
    context.update(rows)
    context_counts.update(counts)
    problems = verify_rows(context, context_counts)
    if problems:
        print("Refusing to write — invariants would break:")
        for p in problems:
            print(f"  ! {p}")
        return 1

    changed = False
    for epoch in targets:
        b_changed = _rewrite_line(BOUNDARIES_CSV, epoch, _boundary_line(rows[epoch]))
        c_changed = _rewrite_line(BLOCKS_CSV, epoch, f"{epoch},{counts[epoch]}")
        diffs = _diff_row(
            epoch, rows[epoch], counts[epoch]
        )  # vs pre-edit committed values
        status = "updated" if (b_changed or c_changed) else "already correct"
        print(
            f"  epoch {epoch}: {status}" + (f"  ({'; '.join(diffs)})" if diffs else "")
        )
        changed = changed or b_changed or c_changed

    print(
        "\nRewrote CSV rows. Review with `git diff` before committing."
        if changed
        else "\nNo changes needed."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="read-only proof + diff")
    parser.add_argument(
        "--repair",
        nargs="+",
        type=int,
        metavar="EPOCH",
        help="regenerate and rewrite the given epoch row(s)",
    )
    parser.add_argument(
        "--ogmios",
        default=DEFAULT_OGMIOS,
        help="Ogmios websocket URL ($OGMIOS_ENDPOINT, else first configured endpoint)",
    )
    parser.add_argument(
        "--kupo",
        default=DEFAULT_KUPO,
        help="optional kupo URL to accelerate boundary lookups "
        "($KUPO_URL; omit for a pure chain-sync walk)",
    )
    args = parser.parse_args()

    if args.repair:
        return asyncio.run(repair(args.ogmios, args.kupo, args.repair))
    if args.verify:
        return asyncio.run(verify(args.ogmios, args.kupo))
    parser.error("choose --verify or --repair")


if __name__ == "__main__":
    raise SystemExit(main())
