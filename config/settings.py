import json
from functools import lru_cache
from typing import TypedDict

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


env_file = ".env"  # ".env.production"


class OgmiosEndpointConfig(TypedDict):
    """Type definition for Ogmios endpoint configuration."""
    url: str
    weight: float


class GeneralSettings(BaseSettings):
    """General application settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )


class DaskSettings(BaseSettings):
    """Dask-related settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    n_workers: int = Field(alias="DASK_N_WORKERS", default=6)
    worker_memory_limit: str = Field(alias="DASK_WORKER_MEMORY_LIMIT", default="3GB")


class MemorySettings(BaseSettings):
    """Adaptive memory controller settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    memory_limit_gb: float = Field(alias="PREFECT_MEMORY_LIMIT_GB", default=24.0)
    warning_threshold: float = Field(alias="MEMORY_WARNING_THRESHOLD", default=0.75)
    critical_threshold: float = Field(alias="MEMORY_CRITICAL_THRESHOLD", default=0.85)
    emergency_threshold: float = Field(alias="MEMORY_EMERGENCY_THRESHOLD", default=0.90)
    check_interval_seconds: int = Field(
        alias="MEMORY_CHECK_INTERVAL_SECONDS", default=10
    )


class RedisSettings(BaseSettings):
    """Redis-related settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    url: str = Field(alias="REDIS_URL", default="redis://localhost:6379/0")
    max_stream_depth: int = Field(alias="REDIS_MAX_STREAM_DEPTH", default=10000)
    warning_threshold: int = Field(alias="REDIS_WARNING_THRESHOLD", default=7500)
    check_interval: int = Field(alias="REDIS_CHECK_INTERVAL", default=10)


class BatchSettings(BaseSettings):
    """Batch size settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    base_size: int = Field(alias="BASE_BATCH_SIZE", default=500)
    min_size: int = Field(alias="MIN_BATCH_SIZE", default=50)
    max_size: int = Field(alias="MAX_BATCH_SIZE", default=1000)


class MonitoringSettings(BaseSettings):
    """Monitoring and metrics settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    snapshot_frequency: int = Field(alias="SNAPSHOT_FREQUENCY", default=5)


class OgmiosSettings(BaseSettings):
    """Ogmios multi-source balancer settings."""

    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    endpoints_str: str = Field(
        alias="OGMIOS_ENDPOINTS",
        default='[{"url": "ws://localhost:1337", "weight": 1.0}]',
    )

    @property
    def endpoints(self) -> list[OgmiosEndpointConfig]:
        """
        Parse the JSON string and return a list of endpoint configurations.
        
        Returns:
            list[OgmiosEndpointConfig]: List of endpoint configurations with 
                url (str) and weight (float) fields.
        """
        parsed_endpoints: list[OgmiosEndpointConfig] = json.loads(self.endpoints_str)
        return parsed_endpoints


@lru_cache
def get_dask_settings() -> DaskSettings:
    return DaskSettings()


@lru_cache
def get_memory_settings() -> MemorySettings:
    return MemorySettings()


@lru_cache
def get_redis_settings() -> RedisSettings:
    return RedisSettings()


@lru_cache
def get_batch_settings() -> BatchSettings:
    return BatchSettings()


@lru_cache
def get_monitoring_settings() -> MonitoringSettings:
    return MonitoringSettings()


@lru_cache
def get_ogmios_settings() -> OgmiosSettings:
    # Set the environment variable for other parts of the app that might use it directly
    # os.environ["OGMIOS_ENDPOINTS"] = OgmiosSettings().endpoints
    return OgmiosSettings()


# Load all settings at startup to ensure they're cached
def load_all_settings() -> None:
    """
    Preload all settings to populate the LRU cache.
    
    This ensures that settings are loaded once at startup rather than 
    on first access, which can help with consistency and performance.
    """
    get_dask_settings()
    get_memory_settings()
    get_redis_settings()
    get_batch_settings()
    get_monitoring_settings()
    get_ogmios_settings()


load_all_settings()
