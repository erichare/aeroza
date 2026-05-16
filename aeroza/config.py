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

    # Cloudflare R2 origin for pre-rendered tile pyramids. When all four
    # ``r2_*`` settings are present, the materialiser prewarm subscriber
    # uploads each rendered tile to ``r2_bucket`` instead of populating
    # the in-process LRU, and the retention worker deletes the
    # corresponding objects on its DB-commit pass. When any value is
    # blank (e.g. local dev without R2), uploads short-circuit to a
    # no-op and the FastAPI tile route falls back to on-demand renders
    # — same shape the API has today.
    #
    # ``r2_endpoint`` is the S3-compatible endpoint Cloudflare hands out
    # at <account-id>.r2.cloudflarestorage.com. ``r2_public_base_url``
    # is the *public* origin the browser hits (custom domain like
    # ``https://tiles.aeroza.app``); the bucket itself is private to
    # writes, public to the CDN. The two are different on purpose so a
    # bucket can be migrated without forcing every client to relearn the
    # endpoint.
    r2_endpoint: str | None = None
    r2_bucket: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_public_base_url: str | None = None

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
