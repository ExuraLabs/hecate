import json
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


env_file = ".env.production"  # ".env"


class DaskSettings(BaseSettings):
    """Dask-related settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    n_workers: int = Field(alias="DASK_N_WORKERS", default=6)
    worker_memory_limit: str = Field(alias="DASK_WORKER_MEMORY_LIMIT", default="3GB")


class ConcurrencySettings(BaseSettings):
    """Concurrency-related settings for optimal performance."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    max_workers: int = Field(alias="MAX_WORKERS", default=6)
    redis_bulk_buffer_size: int = Field(alias="REDIS_BULK_BUFFER_SIZE", default=100)
    
    @property
    def connection_pool_size(self) -> int:
        """Connection pool size matches worker count."""
        return self.max_workers
    
    @property
    def max_concurrent_epochs(self) -> int:
        """Max concurrent epochs matches worker count."""
        return self.max_workers
    
    @property
    def initial_connections(self) -> int:
        """Initial connections equals worker count for immediate availability."""
        return self.max_workers


class MemorySettings(BaseSettings):
    """Memory-related settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    memory_limit_gb: float = Field(alias="MEMORY_LIMIT_GB", default=2.0)
    warning_threshold: float = Field(alias="MEMORY_WARNING_THRESHOLD", default=0.75)
    critical_threshold: float = Field(alias="MEMORY_CRITICAL_THRESHOLD", default=0.85)
    emergency_threshold: float = Field(alias="MEMORY_EMERGENCY_THRESHOLD", default=0.90)
    check_interval_seconds: int = Field(alias="MEMORY_CHECK_INTERVAL", default=30)
    pause_duration_seconds: int = Field(alias="MEMORY_PAUSE_DURATION", default=5)


class RedisSettings(BaseSettings):
    """Redis-related settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    url: str = Field(alias="REDIS_URL", default="redis://localhost:6379/0")
    max_stream_depth: int = Field(alias="REDIS_MAX_STREAM_DEPTH", default=10000)
    warning_threshold: int = Field(alias="REDIS_WARNING_THRESHOLD", default=7500)
    check_interval: int = Field(alias="REDIS_CHECK_INTERVAL", default=30)


class BatchSettings(BaseSettings):
    """Batch size settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    base_size: int = Field(alias="BASE_BATCH_SIZE", default=500)
    min_size: int = Field(alias="MIN_BATCH_SIZE", default=50)
    max_size: int = Field(alias="MAX_BATCH_SIZE", default=1000)


class OgmiosSettings(BaseSettings):
    """Ogmios connection settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    endpoints_str: str = Field(
        alias="OGMIOS_ENDPOINTS",
        default='["ws://localhost:1337"]',
    )

    # Direct connection settings - None means "not configured"
    host: str | None = Field(alias="OGMIOS_HOST", default=None)
    port: str | None = Field(alias="OGMIOS_PORT", default=None)

    @property
    def endpoints(self) -> list[str]:
        """
        Parse the JSON string and return a list of endpoint URLs.

        Returns:
            list[str]: List of WebSocket URLs as simple strings.
        """
        return json.loads(self.endpoints_str)


@lru_cache
def get_dask_settings() -> DaskSettings:
    """Get cached Dask configuration settings."""
    return DaskSettings()


@lru_cache
def get_concurrency_settings() -> ConcurrencySettings:
    """Get cached concurrency settings."""
    return ConcurrencySettings()


@lru_cache
def get_memory_settings() -> MemorySettings:
    """Get cached memory management settings."""
    return MemorySettings()


@lru_cache
def get_redis_settings() -> RedisSettings:
    """Get cached Redis connection and stream settings."""
    return RedisSettings()


@lru_cache
def get_batch_settings() -> BatchSettings:
    """Get cached batch processing size settings."""
    return BatchSettings()


@lru_cache
def get_ogmios_settings() -> OgmiosSettings:
    """
    Get cached Ogmios endpoint configuration settings.
    """
    return OgmiosSettings()


# Load all settings at startup to ensure they're cached
def load_all_settings() -> None:
    """
    Preload all settings to populate the LRU cache.

    This ensures that settings are loaded once at startup rather than
    on first access, which can help with consistency and performance.
    """
    get_dask_settings()
    get_concurrency_settings()
    get_memory_settings()
    get_redis_settings()
    get_batch_settings()
    get_ogmios_settings()


load_all_settings()
