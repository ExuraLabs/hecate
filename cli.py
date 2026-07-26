"""Hecate's command line interface.

    uv run python -m cli backfill --start-epoch 208 --end-epoch 210
    uv run python -m cli status

Everything here is a thin shell over ``backfill.backfill``: argument
parsing, sink selection and logging setup. No orchestration.
"""

import asyncio
import logging
from enum import Enum
from functools import partial
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from backfill import BackfillError, SinkFactory, backfill as run_backfill
from config.log import configure_logging
from constants import FIRST_SHELLEY_EPOCH
from models import EpochNumber

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
    sink: SinkName, *, start_epoch: EpochNumber, prefix: str | None
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
    return partial(
        HistoricalRedisSink,
        start_epoch=start_epoch,
        prefix=prefix or DEFAULT_HISTORY_PREFIX,
    )


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
            "Defaults to the CPU count.",
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
    sink_factory = _sink_factory(sink, start_epoch=start, prefix=redis_prefix)

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
                log_level=level,
            )
        )
    except BackfillError as exc:
        # Already logged per epoch; keep the exit terse and non-zero.
        console.print(f"[bold red]Backfill incomplete:[/] {exc}")
        raise typer.Exit(code=1) from None
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted.[/] Rerun to resume.")
        raise typer.Exit(code=130) from None


@app.command()
def status(
    redis_prefix: Annotated[
        str, typer.Option(help="Key prefix of the epoch-stream sink to report on.")
    ] = DEFAULT_HISTORY_PREFIX,
) -> None:
    """Report how far the epoch-stream Redis sink has been filled."""
    from sinks.redis import HistoricalRedisSink

    async def _status() -> dict[str, object]:
        async with HistoricalRedisSink(prefix=redis_prefix) as sink:
            return await sink.get_status()

    report = asyncio.run(_status())

    table = Table(title=f"Hecate — {redis_prefix}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    for key, value in report.items():
        table.add_row(key.replace("_", " "), str(value))
    console.print(table)


if __name__ == "__main__":
    app()
