"""Local, git-ignored cache of epoch boundaries derived beyond the bootstrap CSVs.

The committed ``data/*.csv`` are a *frozen* bootstrap anchor index; epochs past
them are derived from the chain on demand. This cache persists those derived rows
so repeat runs don't re-derive the same tail — purely an optimization, losing it
only costs a re-derivation. The path is configurable via ``HECATE_EPOCH_CACHE``
(default: a git-ignored file beside the bootstrap data); point it at a persistent
location for deployments with ephemeral checkouts.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path

from models import EpochData, EpochNumber

CACHE_PATH = Path(
    os.getenv(
        "HECATE_EPOCH_CACHE", str(Path(__file__).parent / "data" / "epoch_cache.json")
    )
)


def load_cache() -> tuple[dict[EpochNumber, EpochData], dict[EpochNumber, int]]:
    """Return cached ``(boundaries, block_counts)`` keyed by epoch (empty if none)."""
    if not CACHE_PATH.exists():
        return {}, {}
    raw = json.loads(CACHE_PATH.read_text())
    boundaries = {
        EpochNumber(int(e)): EpochData(**entry["boundary"]) for e, entry in raw.items()
    }
    counts = {EpochNumber(int(e)): int(entry["blocks"]) for e, entry in raw.items()}
    return boundaries, counts


def extend_cache(
    rows: dict[EpochNumber, EpochData], counts: dict[EpochNumber, int]
) -> None:
    """Merge newly derived rows into the cache file, creating it if needed."""
    if not rows:
        return
    raw = json.loads(CACHE_PATH.read_text()) if CACHE_PATH.exists() else {}
    for epoch, boundary in rows.items():
        raw[str(epoch)] = {"boundary": asdict(boundary), "blocks": counts[epoch]}
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(raw, indent=2, sort_keys=True))
