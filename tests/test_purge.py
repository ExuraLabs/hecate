"""Reclaiming a namespace, and the data it refuses to reclaim.

A run purges the epoch streams sitting below its window before it relays
anything. The rule it works to: delete only what is provably finished with.
"No consumer group has registered" is not proof of that — a worker that has
not started yet looks identical on the wire — so those are refused by default
and dropped only on ``--purge-orphans``.
"""

import subprocess
from collections.abc import Callable

from errors import BackfillError, UnsafePurgeError

CLI = Callable[..., subprocess.CompletedProcess[str]]
Publish = Callable[..., None]
HasStream = Callable[[int], bool]
FIRST, SECOND, NEXT = 208, 209, 210


def test_unsafe_purge_is_catchable_as_a_backfill_error() -> None:
    """Sinks raise into the same hierarchy the CLI catches; a purge refusal
    that escaped `except BackfillError` would surface as a raw traceback.
    """
    assert issubclass(UnsafePurgeError, BackfillError)


def test_orphan_streams_survive_a_later_run(
    cli: CLI,
    publish: Publish,
    has_stream: HasStream,
    state: Callable[[], dict[str, object]],
) -> None:
    """Nothing registered to read the published epochs, so the next run stops
    rather than reclaiming them. They are still there afterwards, and the low
    watermark has not moved past them.
    """
    publish(FIRST, SECOND)

    result = cli("backfill", "--start-epoch", str(NEXT), "--end-epoch", str(NEXT))

    assert result.returncode == 12, result.stdout + result.stderr
    assert "no consumer group has registered" in result.stdout
    assert "Backfill incomplete:" in result.stdout, "operator got a bare traceback"
    assert "Traceback" not in result.stderr
    assert has_stream(FIRST)
    assert has_stream(SECOND)
    assert state()["low_watermark"] == FIRST


def test_a_late_consumer_still_gets_what_it_was_owed(
    cli: CLI, publish: Publish, drain: Callable[..., int]
) -> None:
    """The point of the refusal: a worker that turns up after the failed run
    finds its epochs intact. This is the data the default protects.
    """
    publish(FIRST, SECOND, entries=7)
    cli("backfill", "--start-epoch", str(NEXT), "--end-epoch", str(NEXT))

    assert drain([FIRST, SECOND], group="late-worker") == 14


def test_purge_orphans_drops_them_and_names_them(
    cli: CLI, publish: Publish, has_stream: HasStream, ogmios_endpoint: str
) -> None:
    """The opt-in, for a namespace whose consumers genuinely never existed.
    Dropping data is loud and specific about which epochs went.
    """
    publish(FIRST, SECOND)

    result = cli(
        "backfill",
        "--start-epoch",
        str(NEXT),
        "--end-epoch",
        str(NEXT),
        "--purge-orphans",
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert not has_stream(FIRST)
    assert not has_stream(SECOND)
    assert "no registered consumer" in combined
    assert f"{FIRST}–{SECOND}" in combined, "the warning did not say what it dropped"
