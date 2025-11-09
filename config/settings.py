import json

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


env_file = ".env.production"  # ".env"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    url: str = Field(alias="REDIS_URL", default="redis://localhost:6379/0")
    max_stream_depth: int = Field(alias="REDIS_MAX_STREAM_DEPTH", default=10_000)
    warning_threshold: int = Field(alias="REDIS_WARNING_THRESHOLD", default=7_500)
    check_interval: int = Field(alias="REDIS_CHECK_INTERVAL", default=10)


class BatchSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=env_file, env_file_encoding="utf-8", extra="ignore"
    )
    batch_size: int = Field(alias="BATCH_SIZE", default=500)


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
        return json.loads(self.endpoints_str)  # type: ignore[no-any-return]


# Load all settings as module level singletons
redis_settings = RedisSettings()
batch_settings = BatchSettings()
ogmios_settings = OgmiosSettings()
