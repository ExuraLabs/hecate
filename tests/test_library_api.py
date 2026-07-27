"""Hecate used as a library: an import and an await, no CLI, no orchestrator.

This is the surface a third party integrates against, so it is worth pinning
separately from the command line — including the promise that a failed epoch
leaves the sink exactly as consumable as it was before the run.
"""

import asyncio
from functools import partial

import pytest

from backfill import backfill
from constants import FIRST_SHELLEY_EPOCH
from errors import BackfillError, EpochsFailedError
from models import EpochNumber
from sinks.base import EpochCoordinator
from tests.conftest import BASE_EPOCH, PREFIX

# Unlike the other modules, this one names the Redis sinks directly, and they
# live behind an optional dependency group. Skip the file rather than fail
# collection on an install that only has the CLI sink.
pytest.importorskip("redis", reason="the Redis sinks need the redis group")

from sinks.redis import HistoricalRedisSink, RedisSink  # noqa: E402

#: Nothing listens here, so the epoch worker cannot connect.
DEAD_ENDPOINT = "ws://127.0.0.1:1"


def test_backfill_recognises_a_coordinator_by_shape() -> None:
    """Sink selection is structural — a sink is whatever it can do — and this
    is the distinction ``backfill`` branches on: a coordinator gets opened in
    the parent for ordering, a relay-only sink lives entirely in the workers.
    """
    assert isinstance(HistoricalRedisSink(prefix=PREFIX), EpochCoordinator)
    assert not isinstance(RedisSink(), EpochCoordinator)


def test_a_failed_epoch_leaves_the_ordering_base_where_it_was(
    ogmios_endpoint: str,
) -> None:
    """An epoch that fails every attempt must not be reported as delivered.

    The endpoints rotate round-robin: the first connection resolves the chain
    target, so the single epoch worker gets the dead one.
    """

    async def run() -> None:
        factory = partial(HistoricalRedisSink, prefix=PREFIX)
        with pytest.raises(EpochsFailedError) as raised:
            await backfill(
                factory,
                start_epoch=EpochNumber(FIRST_SHELLEY_EPOCH),
                end_epoch=EpochNumber(FIRST_SHELLEY_EPOCH),
                concurrency=1,
                endpoints=[ogmios_endpoint, DEAD_ENDPOINT],
                retries=1,
                retry_delay_seconds=1.0,
            )
        assert isinstance(raised.value, BackfillError)
        assert FIRST_SHELLEY_EPOCH in raised.value.failures

        async with factory() as sink:
            assert await sink.get_last_synced_epoch() == BASE_EPOCH

    asyncio.run(run())
