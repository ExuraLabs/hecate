"""Hecate's command line interface.

    uv run python -m cli backfill --start-epoch 208 --end-epoch 210
    uv run python -m cli status
    uv run python -m cli follow --from-epoch 600 --kupo-url http://localhost:1442

Everything here is a thin shell over ``backfill.backfill`` and
``follow.follow``: argument parsing, sink selection and logging setup. No
orchestration.
"""

import asyncio
import logging
import re
import signal
from enum import Enum
from functools import partial
from typing import Annotated

import typer
from ogmios import Point
from rich.console import Console
from rich.table import Table

from backfill import SinkFactory, backfill as run_backfill
from config.log import configure_logging
from errors import BackfillError, FollowError
from constants import DEFAULT_LIVE_NAMESPACE, FIRST_SHELLEY_EPOCH, SECURITY_PARAMETER
from models import BlockHash, EpochNumber, Slot

_POINT_PATTERN = re.compile(r"(?P<slot>\d+)\.(?P<hash>[0-9a-fA-F]{64})")

#: Each Redis sink keys its own namespace; they are different layouts, so they
#: get different defaults rather than one shared prefix.
DEFAULT_HISTORY_PREFIX = "hecate:history:"
DEFAULT_LIST_PREFIX = "hecate:"

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Relay Cardano block data from an Ogmios endpoint to a sink.",
)
console = Console()


class SinkName(str, Enum):
    """Where relayed blocks should go."""

    #: Per-epoch Redis streams with ordered completion — resumable.
    redis = "redis"
    #: One Redis list of blocks; no ordering contract to opt into.
    redis_list = "redis-list"
    #: Pretty-printed to the terminal.
    cli = "cli"


def _resolve_log_level(name: str) -> int:
    try:
        return logging.getLevelNamesMapping()[name.upper()]
    except KeyError:
        raise typer.BadParameter(
            f"unknown log level {name!r}; try DEBUG, INFO, WARNING or ERROR"
        ) from None


def _sink_factory(
    sink: SinkName, *, prefix: str | None
) -> SinkFactory:  # pragma: no cover - thin wiring
    """Resolve a sink choice to a factory, or exit with a usable message.

    ``prefix`` of None means "whatever that sink's own namespace is" — the
    two Redis sinks have different key layouts and so different defaults.

    Redis is imported lazily because it is an optional dependency group:
    the ``cli`` sink has to keep working in an install without it.
    """
    if sink is SinkName.cli:
        from sinks.cli import CLISink

        return CLISink

    try:
        from sinks.redis import HistoricalRedisSink, RedisSink
    except ImportError:
        console.print(
            f"[bold red]The {sink.value} sink needs the redis extra:[/] "
            "uv sync --group redis"
        )
        raise typer.Exit(code=1) from None

    if sink is SinkName.redis_list:
        return partial(RedisSink, prefix=prefix or DEFAULT_LIST_PREFIX)
    return partial(HistoricalRedisSink, prefix=prefix or DEFAULT_HISTORY_PREFIX)


@app.command()
def backfill(
    start_epoch: Annotated[
        int, typer.Option(help="First epoch to relay.", min=FIRST_SHELLEY_EPOCH)
    ] = FIRST_SHELLEY_EPOCH,
    end_epoch: Annotated[
        int | None,
        typer.Option(
            help="Last epoch to relay, inclusive. "
            "Defaults to the last finalized epoch on chain.",
        ),
    ] = None,
    batch_size: Annotated[
        int | None,
        typer.Option(help="Blocks per batch sent to the sink. [env: BATCH_SIZE]"),
    ] = None,
    concurrency: Annotated[
        int | None,
        typer.Option(
            help="Epochs to fetch in parallel, each in its own process. "
            "Defaults to half the CPU count, since each one holds a whole "
            "epoch of blocks in the sink; raise it only against a sink sized "
            "for that.",
            min=1,
        ),
    ] = None,
    sink: Annotated[
        SinkName, typer.Option(help="Where to relay blocks.")
    ] = SinkName.redis,
    endpoint: Annotated[
        list[str] | None,
        typer.Option(
            help="Ogmios endpoint to fetch from; repeat to rotate across "
            "several. [env: OGMIOS_ENDPOINTS]",
        ),
    ] = None,
    kupo_url: Annotated[
        str | None,
        typer.Option(
            envvar="KUPO_URL",
            help="Optional kupo endpoint, which accelerates deriving boundary "
            "data for epochs absent from the committed CSVs.",
        ),
    ] = None,
    redis_prefix: Annotated[
        str | None,
        typer.Option(
            help="Key prefix for the redis sinks. Defaults to "
            f"'{DEFAULT_HISTORY_PREFIX}' for --sink redis and "
            f"'{DEFAULT_LIST_PREFIX}' for --sink redis-list.",
        ),
    ] = None,
    purge_orphans: Annotated[
        bool,
        typer.Option(
            "--purge-orphans",
            help="Let the startup purge drop already-published epochs below the "
            "window that no consumer group has registered for. Off by "
            "default, because a consumer that has not started yet looks "
            "exactly like one that never will. Use it to reclaim a namespace "
            "whose consumers genuinely never existed.",
        ),
    ] = False,
    rebase_ordering_base: Annotated[
        bool,
        typer.Option(
            "--rebase-ordering-base",
            help="Permit a window starting above the sink's last_synced_epoch, "
            "moving it up to meet --start-epoch. Declares the epochs in "
            "between out of scope — they will never be delivered from this "
            "sink. Without this, such a run is refused, because nothing it "
            "published could become visible.",
        ),
    ] = False,
    log_level: Annotated[str, typer.Option(help="Logging verbosity.")] = "INFO",
) -> None:
    """Relay a range of historical epochs, oldest first.

    Against the redis sink the run is resumable: it continues after the
    last epoch already committed, so rerunning after a failure picks up
    where it left off.
    """
    level = _resolve_log_level(log_level)
    configure_logging(level)

    start = EpochNumber(start_epoch)
    sink_factory = _sink_factory(sink, prefix=redis_prefix)

    try:
        asyncio.run(
            run_backfill(
                sink_factory,
                start_epoch=start,
                end_epoch=EpochNumber(end_epoch) if end_epoch is not None else None,
                batch_size=batch_size,
                concurrency=concurrency,
                endpoints=endpoint or None,
                kupo_url=kupo_url,
                rebase_ordering_base=rebase_ordering_base,
                purge_orphans=purge_orphans,
                log_level=level,
            )
        )
    except BackfillError as exc:
        # Per-epoch failures are already logged; window, purge and ordering
        # problems carry their whole explanation in the message. The exit code
        # is per failure class, so a caller driving this as a subprocess can
        # tell them apart without parsing English — see `errors`.
        console.print(f"[bold red]Backfill incomplete:[/] {exc}")
        raise typer.Exit(code=exc.exit_code) from None
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted.[/] Rerun to resume.")
        raise typer.Exit(code=130) from None


@app.command()
def status(
    redis_prefix: Annotated[
        str, typer.Option(help="Key prefix of the epoch-stream sink to report on.")
    ] = DEFAULT_HISTORY_PREFIX,
) -> None:
    """Report how far the epoch-stream Redis sink has been filled.

    Read-only: this writes nothing, so it is safe against a namespace a
    backfill has not claimed yet.
    """
    from sinks.redis import HistoricalRedisSink

    async def _status() -> dict[str, object]:
        async with HistoricalRedisSink(prefix=redis_prefix) as sink:
            return await sink.get_status()

    report = asyncio.run(_status())
    consistent = report.pop("ordering_consistent", True)

    table = Table(title=f"Hecate — {redis_prefix}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    for key, value in report.items():
        table.add_row(key.replace("_", " "), "—" if value is None else str(value))
    console.print(table)

    if report.get("last_synced_epoch") is None:
        console.print("[dim]Nothing relayed here yet.[/]")
    if not consistent:
        console.print(
            "[bold red]Inconsistent:[/] low_watermark is above "
            "last_synced_epoch + 1, so epochs were published that no consumer "
            "reading up to last_synced_epoch can reach. Flush this namespace, "
            "or relay from last_synced_epoch + 1."
        )


def _parse_point(value: str) -> Point:
    match = _POINT_PATTERN.fullmatch(value)
    if match is None:
        raise typer.BadParameter(
            f"{value!r} is not SLOT.HASH (a slot number, a dot, and a "
            f"64-character block hash)",
            param_hint="--from-point",
        )
    return Point(slot=Slot(int(match["slot"])), id=BlockHash(match["hash"].lower()))


@app.command()
def follow(
    namespace: Annotated[
        str, typer.Option(help="Key namespace of the live stream.")
    ] = DEFAULT_LIVE_NAMESPACE,
    from_point: Annotated[
        str | None,
        typer.Option(
            metavar="SLOT.HASH",
            help="Start an empty stream right after this block.",
        ),
    ] = None,
    from_slot: Annotated[
        int | None,
        typer.Option(
            min=0,
            help="Start an empty stream right after the last block at or "
            "before this slot. Needs --kupo-url.",
        ),
    ] = None,
    from_epoch: Annotated[
        int | None,
        typer.Option(
            min=FIRST_SHELLEY_EPOCH,
            help="Start an empty stream at the first block of this epoch. "
            "Needs --kupo-url.",
        ),
    ] = None,
    kupo_url: Annotated[
        str | None,
        typer.Option(
            envvar="KUPO_URL",
            help="Kupo endpoint that resolves --from-slot and --from-epoch.",
        ),
    ] = None,
    endpoint: Annotated[
        list[str] | None,
        typer.Option(
            help="Ogmios endpoint to follow; repeat to fail over across several "
            "on reconnect. [env: OGMIOS_ENDPOINTS]",
        ),
    ] = None,
    depth: Annotated[
        int,
        typer.Option(
            min=0,
            help="Blocks held back until that many more are built on them. "
            "Rollbacks no deeper never reach the stream.",
        ),
    ] = 2,
    max_catchup_epochs: Annotated[
        int,
        typer.Option(
            min=0,
            help="Refuse to start further than this many epochs behind the "
            "node's tip; relay that stretch with backfill instead.",
        ),
    ] = 2,
    in_flight: Annotated[
        int, typer.Option(min=1, help="nextBlock requests kept outstanding.")
    ] = 50,
    lease_seconds: Annotated[
        float,
        typer.Option(
            help="Producer lease lifetime. A standby takes over this long after "
            "the writer stops renewing it."
        ),
    ] = 15.0,
    heartbeat_seconds: Annotated[
        float, typer.Option(help="How often the lease and state are refreshed.")
    ] = 2.0,
    max_unconsumed_blocks: Annotated[
        int,
        typer.Option(
            help="Pause while the slowest active consumer trails the tail by "
            "more block entries than this."
        ),
    ] = 10_000,
    retain_blocks: Annotated[
        int,
        typer.Option(help="Block entries always kept behind the tail."),
    ] = SECURITY_PARAMETER,
    max_retained_blocks: Annotated[
        int,
        typer.Option(
            help="Block entries a stale consumer can hold back from trimming."
        ),
    ] = 20 * SECURITY_PARAMETER,
    active_consumer_seconds: Annotated[
        float,
        typer.Option(
            help="A consumer that committed within this long is active: it "
            "pins retention and applies backpressure."
        ),
    ] = 600.0,
    log_level: Annotated[str, typer.Option(help="Logging verbosity.")] = "INFO",
) -> None:
    """Follow the chain tip into the live Redis stream, until stopped.

    A stream that already has entries resumes from its own tail, and the
    start options are for its first run only. See docs/live-stream.md.
    SIGTERM or Ctrl-C stops it cleanly, releasing the producer lease.
    """
    level = _resolve_log_level(log_level)
    configure_logging(level)

    try:
        from follow import follow as run_follow, resolve_start_point
        from sinks.redis_live import LivePolicy
    except ImportError:
        console.print(
            "[bold red]follow needs the redis extra:[/] uv sync --group redis"
        )
        raise typer.Exit(code=1) from None

    try:
        policy = LivePolicy(
            lease_seconds=lease_seconds,
            heartbeat_seconds=heartbeat_seconds,
            max_unconsumed_blocks=max_unconsumed_blocks,
            retain_blocks=retain_blocks,
            max_retained_blocks=max_retained_blocks,
            active_consumer_seconds=active_consumer_seconds,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None

    try:
        start = resolve_start_point(
            point=_parse_point(from_point) if from_point is not None else None,
            slot=from_slot,
            epoch=from_epoch,
            kupo_url=kupo_url,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    except FollowError as exc:
        console.print(f"[bold red]Follow stopped:[/] {exc}")
        raise typer.Exit(code=exc.exit_code) from None

    async def _follow() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await run_follow(
            namespace=namespace,
            start=start,
            endpoints=endpoint or None,
            depth=depth,
            in_flight=in_flight,
            max_catchup_epochs=max_catchup_epochs,
            policy=policy,
            stop=stop,
        )

    try:
        asyncio.run(_follow())
    except FollowError as exc:
        # As with backfill: the exit code is the contract for supervisors, and
        # the message is for whoever reads the journal.
        console.print(f"[bold red]Follow stopped:[/] {exc}")
        raise typer.Exit(code=exc.exit_code) from None


if __name__ == "__main__":
    app()
