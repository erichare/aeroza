"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AEROZA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Literal["development", "staging", "production", "test"] = "development"
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8000

    database_url: str = Field(
        default="postgresql+asyncpg://aeroza:aeroza@localhost:5432/aeroza",
        description="SQLAlchemy async DSN for Postgres + PostGIS.",
    )
    redis_url: str = "redis://localhost:6379/0"
    nats_url: str = "nats://localhost:4222"

    s3_endpoint: str | None = None
    s3_bucket: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = "us-east-1"

    api_key_salt: str = "dev-only-replace-me"

    # Comma-separated list of additional CORS origins to allow on top of
    # the dev-console default. Set this on production deployments where
    # the web frontend lives on a different host than the API (e.g. a
    # Vercel-hosted dashboard talking to a Railway-hosted API). Example:
    # ``AEROZA_CORS_ALLOW_ORIGINS="https://aeroza.vercel.app,https://aeroza.dev"``
    cors_allow_origins: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
