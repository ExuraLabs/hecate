"""Failures a backfill can raise.

One hierarchy, in one module, so ``except BackfillError`` catches everything a
run can fail with — including the failures raised from inside a sink, which is
why these do not live in ``backfill.py``.

Each class carries the process exit code the CLI uses for it. **That code is
the stable contract for anything driving Hecate as a subprocess** — the message
text is written for people to read and may be reworded, so classify on the
code, not on the prose.
"""

from models import EpochNumber


class BackfillError(RuntimeError):
    """Base class for every way a backfill can fail.

    Exit codes start at 10 to stay clear of 1 and 2, which the shell and
    Click already spend on generic and usage failures.
    """

    #: Process exit code the CLI reports for this failure.
    exit_code: int = 1


class EpochsFailedError(BackfillError):
    """One or more epochs could not be relayed, even after retries.

    Raised after the surviving epochs in the batch have been committed, so
    ``last_synced_epoch`` still marks a contiguous, consumable prefix and a
    rerun picks up from the first epoch that failed.

    The only failure here worth retrying blindly: the cause is usually
    transient, and a rerun resumes rather than repeating work.
    """

    exit_code = 10

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

    exit_code = 11

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

    exit_code = 14

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
    because the alternative is losing epochs a consumer is still owed. The two
    subclasses are separate because their remedies are: one needs a reader to
    turn up, the other needs the reader it has to finish.
    """

    def __init__(self, epoch: int, stream_key: str, reason: str):
        self.epoch = epoch
        self.stream_key = stream_key
        super().__init__(f"cannot purge epoch {epoch} ({stream_key}): {reason}")


class NoRegisteredConsumerError(UnsafePurgeError):
    """Published epochs that nothing has registered to read.

    Retrying is pointless — nothing about the run will change this. Either a
    consumer needs to register, or the operator has to say the data is
    disposable with ``--purge-orphans``.
    """

    exit_code = 12

    def __init__(self, epoch: int, stream_key: str):
        super().__init__(
            epoch,
            stream_key,
            "it holds data but no consumer group has registered. A consumer "
            "that has not started yet looks exactly like one that never will, "
            "so this is not assumed to be abandoned. Start the consumers that "
            "should read it, or pass purge_orphans=True (--purge-orphans) to "
            "drop it.",
        )


class ConsumerNotFinishedError(UnsafePurgeError):
    """A registered consumer has not finished with these epochs yet.

    Waiting is the remedy: retry once the consumer has drained.
    """

    exit_code = 13

    def __init__(self, epoch: int, stream_key: str, groups: int):
        self.groups = groups
        super().__init__(
            epoch,
            stream_key,
            f"it has unconsumed data with {groups} active consumer group(s). "
            f"Let consumers finish, or FLUSHDB to start from scratch.",
        )


__all__ = [
    "BackfillError",
    "ConsumerNotFinishedError",
    "EpochsFailedError",
    "NoRegisteredConsumerError",
    "OrderingStalledError",
    "UnreachableWindowError",
    "UnsafePurgeError",
]
