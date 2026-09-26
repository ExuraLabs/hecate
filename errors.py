"""Failures a backfill or a live follow can raise.

One hierarchy, in one module, so ``except BackfillError`` catches everything a
backfill can fail with and ``except FollowError`` everything a follow can —
including the failures raised from inside a sink or the Ogmios client, which is
why these do not live beside either entry point.

Each class carries the process exit code the CLI uses for it. **That code is
the stable contract for anything driving Hecate as a subprocess** — the message
text is written for people to read and may be reworded, so classify on the
code, not on the prose.
"""

from collections.abc import Sequence
from typing import Any

from models import EpochNumber


class HecateError(RuntimeError):
    """Root of every failure Hecate reports with its own exit code.

    Exit codes start at 10 to stay clear of 1 and 2, which the shell and
    Click already spend on generic and usage failures.
    """

    #: Process exit code the CLI reports for this failure.
    exit_code: int = 1


class BackfillError(HecateError):
    """Base class for every way a backfill can fail."""


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


class FollowError(HecateError):
    """Base class for every way a live follow can stop short of being told to.

    Codes continue where the backfill's leave off.
    """


def _describe_point(point: Any) -> str:
    return f"{point.slot}.{point.id}"


class IntersectionNotFoundError(FollowError):
    """Ogmios holds none of the points the follower asked to start from.

    The node's tip is past all of them, so this is not a node that is still
    syncing: the stream's recent history is not on the node's chain at all —
    a different network, or a fork deeper than the points offered.
    """

    exit_code = 15

    def __init__(self, *, points: Sequence[Any], tip: Any):
        self.points = list(points)
        self.tip = tip
        offered = ", ".join(_describe_point(point) for point in self.points[:3])
        more = f" and {len(self.points) - 3} older" if len(self.points) > 3 else ""
        super().__init__(
            f"the node (tip {_describe_point(tip)}) holds none of the "
            f"{len(self.points)} point(s) offered: {offered}{more}. Check that "
            f"Ogmios serves the network this stream was built from."
        )


class FencedOutError(FollowError):
    """This follower may no longer write to the stream.

    Every write re-checks that the follower still holds the lease and that the
    stream's tail is where this follower left it. Either failing means another
    writer may have moved the stream, so nothing this process believes about
    the tail can be trusted. Restarting is the remedy: the new process waits as
    standby and re-reads the tail if it takes over.
    """

    exit_code = 16


class LeaseLostError(FencedOutError):
    """The producer lease expired or is held by another follower."""

    def __init__(self, *, producer_id: str, holder: str | None):
        self.producer_id = producer_id
        self.holder = holder
        held = f"is held by {holder}" if holder else "has expired"
        super().__init__(
            f"the producer lease {held}; {producer_id} is no longer the writer"
        )


class TailMovedError(FencedOutError):
    """The stream's tail is not where this follower last wrote it."""

    def __init__(self, *, expected: str, found: str | None):
        self.expected = expected
        self.found = found
        super().__init__(
            f"the stream's tail is {found or 'unset'}, not {expected}: "
            f"something other than this follower wrote to it"
        )


class StartPointRefusedError(FollowError):
    """No acceptable point to start following from.

    Raised before anything is written: nothing was asked for, the point asked
    for contradicts the stream, or it is further behind the tip than the
    follower was allowed to catch up.
    """

    exit_code = 17

    def __init__(self, reason: str):
        super().__init__(f"refusing to start: {reason}")


class ChainLinkError(FollowError):
    """A block or rollback does not fit the chain relayed so far.

    Each block names its predecessor; one naming anything else, or a rollback
    to a point that cannot be on the chain relayed so far, means the upstream
    broke the chain-sync contract. Nothing is published from the break onwards.
    """

    exit_code = 18

    def __init__(self, detail: str):
        super().__init__(f"chain-link violation: {detail}")


__all__ = [
    "BackfillError",
    "ChainLinkError",
    "ConsumerNotFinishedError",
    "EpochsFailedError",
    "FencedOutError",
    "FollowError",
    "HecateError",
    "IntersectionNotFoundError",
    "LeaseLostError",
    "NoRegisteredConsumerError",
    "OrderingStalledError",
    "StartPointRefusedError",
    "TailMovedError",
    "UnreachableWindowError",
    "UnsafePurgeError",
]
