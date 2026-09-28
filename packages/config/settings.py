from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WOE_", env_file=".env", extra="ignore")

    log_level: str = "INFO"
    kafka_bootstrap_servers: str = "localhost:9092"
    outbox_poll_interval_seconds: float = Field(default=0.25, gt=0)


class ApiSettings(CommonSettings):
    api_database_url: str = "postgresql+asyncpg://woe:woe@localhost:5433/woe_api"
    api_host: str = "0.0.0.0"
    api_port: int = 8000


class EngineSettings(CommonSettings):
    engine_metrics_port: int = Field(default=9101, ge=1, le=65535)
    engine_database_url: str = "postgresql+asyncpg://woe:woe@localhost:5434/woe_engine"
    retry_poll_interval_seconds: float = Field(default=0.25, gt=0)


class WorkerSettings(CommonSettings):
    worker_metrics_port: int = Field(default=9102, ge=1, le=65535)
    worker_database_url: str = "postgresql+asyncpg://woe:woe@localhost:5435/woe_worker"
    worker_processing_lease_seconds: int = Field(default=60, gt=0)


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()


@lru_cache
def get_engine_settings() -> EngineSettings:
    return EngineSettings()


@lru_cache
def get_worker_settings() -> WorkerSettings:
    return WorkerSettings()
