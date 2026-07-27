"""Failures a backfill can raise.

One hierarchy, in one module, so ``except BackfillError`` catches everything a
run can fail with — including the failures raised from inside a sink, which is
why these do not live in ``backfill.py``.
"""

from models import EpochNumber


class BackfillError(RuntimeError):
    """Base class for every way a backfill can fail."""


class EpochsFailedError(BackfillError):
    """One or more epochs could not be relayed, even after retries.

    Raised after the surviving epochs in the batch have been committed, so
    ``last_synced_epoch`` still marks a contiguous, consumable prefix and a
    rerun picks up from the first epoch that failed.
    """

    def __init__(self, failures: dict[EpochNumber, BaseException]):
        self.failures = failures
        listed = ", ".join(str(epoch) for epoch in sorted(failures))
        super().__init__(f"{len(failures)} epoch(s) failed after retries: {listed}")


class UnreachableWindowError(BackfillError):
    """The sink's ordering base sits below the window we were asked to relay.

    Ordered completion advances one epoch at a time from the base, so it can
    never step over the gap: every epoch such a run relayed would land in the
    sink marked ready and stay unreachable, while the run reported success.
    Refused before anything is written.
    """

    def __init__(self, *, base: EpochNumber, start_epoch: EpochNumber):
        self.base = base
        self.start_epoch = start_epoch
        super().__init__(
            f"sink's last_synced_epoch is {base}, but relaying from "
            f"{start_epoch} needs it at {start_epoch - 1}: epochs "
            f"{base + 1}–{start_epoch - 1} were never delivered here, so "
            f"nothing this run published could become visible. Either relay "
            f"from {base + 1}, or pass rebase_ordering_base=True "
            f"(--rebase-ordering-base) to write those epochs off as out of "
            f"scope."
        )


class OrderingStalledError(BackfillError):
    """Every epoch relayed, but the sink's delivered mark did not reach them.

    A guard against silently publishing data no consumer can reach: the
    delivered mark is the one field consumers bound their reads by, so a run
    must never report success while it lags the epochs just written.
    """

    def __init__(self, *, last_synced: EpochNumber | None, target: EpochNumber):
        self.last_synced = last_synced
        self.target = target
        super().__init__(
            f"relayed through epoch {target}, but the sink reports "
            f"last_synced_epoch={last_synced}: epochs are stranded where no "
            f"consumer can read them. This is a bug, not a config problem — "
            f"please report the sink's status output."
        )


class UnsafePurgeError(BackfillError):
    """Reclaiming already-published data would take it from a consumer.

    Raised before anything is deleted. The run aborts rather than proceeding,
    because the alternative is losing epochs a consumer is still owed.
    """

    def __init__(self, epoch: int, stream_key: str, reason: str):
        self.epoch = epoch
        self.stream_key = stream_key
        super().__init__(f"cannot purge epoch {epoch} ({stream_key}): {reason}")


__all__ = [
    "BackfillError",
    "EpochsFailedError",
    "OrderingStalledError",
    "UnreachableWindowError",
    "UnsafePurgeError",
]
