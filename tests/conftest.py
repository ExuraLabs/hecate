"""Fixtures for Hecate's test suite.

The suite is deliberately integration-flavoured. What it checks — the
Lua-atomic epoch advance, consumer-group registration, stream purging — *is*
Redis semantics, so a fake would only assert that the fake matches itself.

That makes a Redis the one hard requirement, and it is met in whichever way
suits the machine::

    # bring your own — a container, a remote instance, anything
    HECATE_TEST_REDIS_URL=redis://localhost:6390/0 uv run pytest

    # or let the suite start and own a private `redis-server` on a free port
    uv run pytest

Neither available, or the ``redis`` dependency group not installed, and
everything here skips rather than erroring: none of it means anything
without a Redis to be wrong about.

Relaying real blocks is the expensive part, so most tests fake an
already-finished relay with the ``publish`` fixture and exercise the
pre-flight checks, which run before a single block is fetched. Only the ones
that must see blocks move need Ogmios::

    HECATE_TEST_OGMIOS=ws://your-server:1337 uv run pytest

Without that variable those tests skip; the rest still run.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from config import settings

if TYPE_CHECKING:
    import redis

REPO_ROOT = Path(__file__).resolve().parent.parent

#: The Redis client ships in an optional dependency group, so importing it is
#: a runtime decision rather than a module-level one. A base install runs the
#: CLI sink and nothing here.
HAS_REDIS_CLIENT = importlib.util.find_spec("redis") is not None

#: Early Shelley epochs are the cheapest real ones to relay — same code path
#: as any other epoch, a fraction of the blocks. 207 is the ordering base a
#: run starting at FIRST_SHELLEY_EPOCH expects to find.
BASE_EPOCH = 207

PREFIX = "hecate:history:"

#: What ``xreadgroup`` hands back on a ``decode_responses=True`` client. The
#: library types every command for both the sync and async clients at once,
#: so the sync return types arrive as unions that need narrowing.
StreamRead = list[tuple[str, list[tuple[str, dict[str, str]]]]]


def _client(url: str) -> redis.Redis:
    import redis

    return redis.Redis.from_url(url, decode_responses=True)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _adopt(url: str) -> Iterator[str]:
    """Borrow a Redis the operator pointed us at, if it is safe to empty.

    Every test starts from an empty database, so running against one holding
    anything would destroy someone's data on the first test. Refuse loudly
    rather than guess which keys were meant to survive — and hand it back
    empty, which is both how we found it and what lets the next run start.
    """
    import redis

    client = _client(url)
    try:
        try:
            existing = client.dbsize()
        except redis.RedisError as exc:
            pytest.skip(f"HECATE_TEST_REDIS_URL {url} is unreachable: {exc}")
        if existing:
            pytest.exit(
                f"HECATE_TEST_REDIS_URL points at {url}, which holds "
                f"{existing} key(s). This suite empties its Redis between "
                f"tests — point it at a scratch instance or an unused "
                f"database number.",
                returncode=1,
            )
        try:
            yield url
        finally:
            client.flushdb()
    finally:
        client.close()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A Redis the suite may empty at will, however the machine can supply one."""
    if not HAS_REDIS_CLIENT:
        pytest.skip("the redis client is not installed (uv sync --group redis)")

    provided = os.environ.get("HECATE_TEST_REDIS_URL")
    if provided:
        yield from _adopt(provided)
        return

    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip(
            "no Redis available: set HECATE_TEST_REDIS_URL, or install "
            "redis-server for the suite to start one itself"
        )

    port = _free_port()
    url = f"redis://127.0.0.1:{port}/0"
    server = subprocess.Popen(
        [binary, "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_until_up(url, server, port)
        yield url
    finally:
        server.terminate()
        server.wait(timeout=10)


def _wait_until_up(url: str, server: subprocess.Popen[bytes], port: int) -> None:
    import redis

    client = _client(url)
    deadline = time.monotonic() + 10
    try:
        while True:
            try:
                client.ping()
                return
            except redis.ConnectionError:
                if time.monotonic() > deadline or server.poll() is not None:
                    raise RuntimeError(
                        f"redis-server never came up on {port}"
                    ) from None
                time.sleep(0.05)
    finally:
        client.close()


@pytest.fixture(scope="session")
def ogmios_endpoint() -> str:
    """A live Ogmios to relay from, or a skip for the tests that need one."""
    url = os.environ.get("HECATE_TEST_OGMIOS")
    if not url:
        pytest.skip("set HECATE_TEST_OGMIOS to a reachable Ogmios endpoint")
    host, _, port = url.rsplit("/", 1)[-1].rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=5):
            pass
    except OSError as exc:
        pytest.skip(f"Ogmios at {url} is unreachable: {exc}")
    return url


@pytest.fixture(autouse=True)
def hecate_env(monkeypatch: pytest.MonkeyPatch, redis_url: str) -> Iterator[None]:
    """Point Hecate at the test services, and start each test empty.

    Both halves are needed: ``setenv`` reaches the spawned worker processes,
    which re-import ``settings`` from the environment, while ``setattr``
    reaches this one, which imported it long ago.
    """
    endpoint = os.environ.get("HECATE_TEST_OGMIOS", "ws://localhost:1337")
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("OGMIOS_ENDPOINTS", json.dumps([endpoint]))
    monkeypatch.setattr(settings, "REDIS_URL", redis_url)
    monkeypatch.setattr(settings, "OGMIOS_ENDPOINTS", (endpoint,))

    client = _client(redis_url)
    client.flushdb()
    client.close()
    yield


@pytest.fixture
def conn(redis_url: str) -> Iterator[redis.Redis]:
    """A plain synchronous client, for looking at what Hecate left behind.

    The fixtures below are the intended way in — a test that reaches for this
    directly makes its module depend on the optional Redis client.
    """
    client = _client(redis_url)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def cli() -> Callable[..., subprocess.CompletedProcess[str]]:
    """Run the CLI the way an operator does: as a subprocess, for its exit code."""

    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "cli", *args],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )

    return run


@pytest.fixture
def state(conn: redis.Redis) -> Callable[[], dict[str, object]]:
    """The three fields that decide what a consumer can see."""

    def read() -> dict[str, object]:
        def epoch(key: str) -> int | None:
            raw = cast("str | None", conn.get(f"{PREFIX}{key}"))
            return None if raw is None else int(raw)

        ready = cast("set[str]", conn.smembers(f"{PREFIX}ready_set"))
        return {
            "last_synced": epoch("last_synced_epoch"),
            "low_watermark": epoch("low_watermark"),
            "ready_set": sorted(int(member) for member in ready),
        }

    return read


@pytest.fixture
def seed_base(conn: redis.Redis) -> Callable[..., None]:
    """Put an ordering base in place without relaying anything to earn it.

    ``low_watermark`` defaults to where a seeded namespace starts out, one
    above the base; pass it explicitly to forge a namespace no consumer
    could drain.
    """

    def write(base: int, *, low_watermark: int | None = None) -> None:
        conn.set(f"{PREFIX}last_synced_epoch", base)
        conn.set(
            f"{PREFIX}low_watermark",
            base + 1 if low_watermark is None else low_watermark,
        )

    return write


@pytest.fixture
def publish(conn: redis.Redis) -> Callable[..., None]:
    """Fake a finished relay of ``epochs``, without fetching a single block.

    Writes what a completed run leaves behind — populated per-epoch streams,
    the low watermark at the bottom of them and the ordering base at the top.
    Everything the pre-flight checks read, and nothing else, so the tests
    that only exercise those cost nothing.
    """

    def write(*epochs: int, entries: int = 5) -> None:
        for epoch in epochs:
            for index in range(entries):
                conn.xadd(
                    f"{PREFIX}epoch:{epoch}",
                    {"type": "batch", "epoch": epoch, "seq": index},
                )
        conn.set(f"{PREFIX}last_synced_epoch", max(epochs))
        conn.set(f"{PREFIX}low_watermark", min(epochs))

    return write


@pytest.fixture
def has_stream(conn: redis.Redis) -> Callable[[int], bool]:
    """Whether an epoch's stream is still in the namespace."""

    def exists(epoch: int) -> bool:
        return bool(conn.exists(f"{PREFIX}epoch:{epoch}"))

    return exists


@pytest.fixture
def drain(conn: redis.Redis) -> Callable[..., int]:
    """Stand in for a consumer: register a group, read everything, ack it.

    Returns the number of entries read. A stream drained this way is provably
    finished with, which is what lets the next run's purge reclaim it without
    ``--purge-orphans``.
    """
    import redis

    def consume(epochs: Iterable[int], group: str = "test-consumer") -> int:
        read = 0
        for epoch in epochs:
            key = f"{PREFIX}epoch:{epoch}"
            if not conn.exists(key):
                continue
            try:
                conn.xgroup_create(key, group, id="0")
            except redis.ResponseError:
                pass  # already registered
            while True:
                batches = cast(
                    StreamRead, conn.xreadgroup(group, "w1", {key: ">"}, count=100)
                )
                if not batches or not batches[0][1]:
                    break
                for entry_id, _ in batches[0][1]:
                    conn.xack(key, group, entry_id)
                    read += 1
        return read

    return consume
