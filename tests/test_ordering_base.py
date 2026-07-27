"""The ordering base: the one field a consumer bounds its reads by.

Ordered completion walks ``last_synced_epoch`` up one epoch at a time, so a
run relaying from N needs it sitting at exactly N-1. Everything here is a way
of getting that wrong, and what Hecate is supposed to do about it.

Most of these cost nothing: the checks run before a block is fetched, so the
"already relayed" state is faked rather than earned. The three that carry a
window through to real blocks ask for ``ogmios_endpoint`` and skip without it.
"""

import subprocess
from collections.abc import Callable

from constants import FIRST_SHELLEY_EPOCH
from tests.conftest import BASE_EPOCH

CLI = Callable[..., subprocess.CompletedProcess[str]]
State = Callable[[], dict[str, object]]
Drain = Callable[..., int]


def test_status_writes_nothing(cli: CLI, state: State) -> None:
    """`status` is a reader. Seeding a base from it is what caused a live bug:
    a namespace nobody had relayed into came out claiming epoch 207 delivered.
    """
    result = cli("status")

    assert result.returncode == 0
    assert state() == {"last_synced": None, "low_watermark": None, "ready_set": []}
    assert "Nothing relayed here yet" in result.stdout


def test_status_flags_a_namespace_no_consumer_can_drain(
    cli: CLI, seed_base: Callable[..., None]
) -> None:
    """low_watermark above last_synced_epoch + 1 means epochs were published
    that nobody reading in order can reach. Report it rather than tabulate it.
    """
    seed_base(BASE_EPOCH, low_watermark=501)

    result = cli("status")

    assert "Inconsistent" in result.stdout


def test_window_above_the_base_is_refused(
    cli: CLI, seed_base: Callable[..., None], state: State
) -> None:
    """The gap can never be walked over, so every epoch this run published
    would sit unreachable while the run reported success. Refuse instead.
    """
    seed_base(BASE_EPOCH)

    result = cli("backfill", "--start-epoch", "210", "--end-epoch", "211")

    assert result.returncode == 11, result.stdout + result.stderr
    assert "never delivered here" in result.stdout
    assert state()["last_synced"] == BASE_EPOCH


def test_chunks_refuse_when_nothing_has_consumed_the_last_one(
    cli: CLI, publish: Callable[..., None], has_stream: Callable[[int], bool]
) -> None:
    """Seeding chunk by chunk with no consumer running: the second chunk would
    have to drop the first one's streams to proceed, so it stops instead.
    """
    publish(FIRST_SHELLEY_EPOCH)

    result = cli("backfill", "--start-epoch", "209", "--end-epoch", "209")

    assert result.returncode == 12, result.stdout + result.stderr
    assert "no consumer group has registered" in result.stdout
    assert has_stream(FIRST_SHELLEY_EPOCH)


def test_the_bounded_window_that_was_reported(
    cli: CLI, state: State, ogmios_endpoint: str
) -> None:
    """The original repro: `status` on a fresh namespace, then a bounded run.

    It used to fail because `status` had seeded the base underneath it.
    """
    cli("status")

    result = cli(
        "backfill",
        "--start-epoch",
        str(FIRST_SHELLEY_EPOCH),
        "--end-epoch",
        str(FIRST_SHELLEY_EPOCH),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert state()["last_synced"] == FIRST_SHELLEY_EPOCH


def test_rebase_carries_a_window_over_the_gap(
    cli: CLI, seed_base: Callable[..., None], state: State, ogmios_endpoint: str
) -> None:
    """The override for when the operator means it: the epochs in between are
    declared out of scope, and the base moves up to meet the window.
    """
    seed_base(BASE_EPOCH)

    result = cli(
        "backfill",
        "--start-epoch",
        "210",
        "--end-epoch",
        "210",
        "--rebase-ordering-base",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert state()["last_synced"] == 210
    assert "written off as out of scope" in result.stdout + result.stderr


def test_an_already_delivered_start_is_narrowed_away(
    cli: CLI,
    publish: Callable[..., None],
    drain: Drain,
    has_stream: Callable[[int], bool],
    state: State,
    ogmios_endpoint: str,
) -> None:
    """Asking again for an epoch already delivered must not relay it twice —
    a duplicate would land above the base and strand there. The window slides
    up, loudly, and the drained stream below it is reclaimed.
    """
    publish(FIRST_SHELLEY_EPOCH)
    drain([FIRST_SHELLEY_EPOCH])

    result = cli(
        "backfill", "--start-epoch", str(FIRST_SHELLEY_EPOCH), "--end-epoch", "209"
    )
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "Narrowing the window" in combined
    assert state()["last_synced"] == 209
    assert not has_stream(FIRST_SHELLEY_EPOCH), (
        "epoch 208 was relayed again instead of being left behind"
    )


def test_chunks_land_contiguously_when_a_consumer_drains_between(
    cli: CLI,
    drain: Drain,
    state: State,
    ogmios_endpoint: str,
) -> None:
    """The path a chunked seed actually takes: relay a chunk, let the consumer
    finish, relay the next. No flags, no gaps, base advancing the whole way.
    """
    first = cli(
        "backfill",
        "--start-epoch",
        str(FIRST_SHELLEY_EPOCH),
        "--end-epoch",
        str(FIRST_SHELLEY_EPOCH),
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert drain([FIRST_SHELLEY_EPOCH]) > 0

    second = cli("backfill", "--start-epoch", "209", "--end-epoch", "209")

    assert second.returncode == 0, second.stdout + second.stderr
    assert state() == {"last_synced": 209, "low_watermark": 209, "ready_set": []}
