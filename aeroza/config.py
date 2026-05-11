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

    # Retention windows for the prune worker. ``mrms_retention_hours``
    # drops observation Zarrs (and the nowcasts derived from them) whose
    # source-file ``valid_at`` is older than the cutoff — this is what
    # keeps the Railway volume from filling. Six hours leaves a comfortable
    # margin over the 60-minute max forecast horizon the verifier needs.
    # ``alert_retention_days`` drops expired ``nws_alerts`` rows; they're
    # small but accumulate, so a 30-day default suits the historical-alerts
    # endpoint without growing unbounded.
    mrms_retention_hours: float = 6.0
    alert_retention_days: int = 30
    # Cadence + batch size for the prune worker. Default cadence is
    # well below the MRMS arrival rate (~30 frames/hour), so the worker
    # is never the bottleneck. Batching caps the per-tick transaction so
    # a long-running prune (e.g. after a deploy gap) doesn't lock writes.
    retention_interval_seconds: float = 600.0
    retention_batch_size: int = 500


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
