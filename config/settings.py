"""
Environment-backed configuration for Hecate.

Values come from the process environment, optionally seeded from a dotenv file
(``.env`` at the repo root by default; point ``HECATE_ENV_FILE`` at another one
to pick per-environment files).  Real environment variables always win over
dotenv entries, so a parent process can configure workers programmatically via
``os.environ`` before import.

Settings are read once, at import time.  ``ProcessPoolExecutor`` workers created
with the ``spawn`` start method re-import this module and re-read the same
environment, so parent and child agree without any explicit hand-off.

Import the module, not the names — ``from config import settings`` then
``settings.REDIS_URL``, so a test can swap a value without every importer
having already bound it.
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Anchored to the repo root rather than the CWD so spawned workers and
# invocations from a subdirectory resolve the same file.
_REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = Path(os.environ.get("HECATE_ENV_FILE") or _REPO_ROOT / ".env")

# override=False: real env vars beat dotenv entries.
# A missing file is a silent no-op.
load_dotenv(ENV_FILE, encoding="utf-8", override=False)


class ConfigError(ValueError):
    """Raised when an environment variable holds an unusable value."""


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_json_str_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"{name} must be a JSON array of strings, e.g. '[\"ws://host:1337\"]'; "
            f"got {raw!r}"
        ) from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{name} must be a JSON array of strings, got {value!r}")
    return tuple(value)


#: Ogmios endpoints to relay from, rotated round-robin.
OGMIOS_ENDPOINTS: tuple[str, ...] = _env_json_str_tuple(
    "OGMIOS_ENDPOINTS", ("ws://localhost:1337",)
)

#: Where the Redis sinks connect.
REDIS_URL: str = _env_str("REDIS_URL", "redis://localhost:6379/0")

#: Published-but-unconsumed epochs tolerated before a backfill pauses.
REDIS_MAX_UNCONSUMED_EPOCHS: int = _env_int("REDIS_MAX_UNCONSUMED_EPOCHS", 10)

#: Blocks per batch handed to a sink.
BATCH_SIZE: int = _env_int("BATCH_SIZE", 500)
