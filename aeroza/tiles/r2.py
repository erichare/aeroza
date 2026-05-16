"""Cloudflare R2 client for the pre-rendered tile pyramid.

The materialiser prewarm subscriber renders the full CONUS tile pyramid
(z=2..8) for every newly-materialised MRMS grid and uploads each tile
to R2 under ``{file_key}/{z}/{x}/{y}.{webp|png}``. The frontend then
fetches tiles directly from a Cloudflare-CDN-fronted public URL on R2,
which is unbounded-parallel, sub-100ms, and immortal-cacheable — the
architectural fix for the "32% 5xx under burst" pattern PR #91 + #92
patched without fully resolving.

This module is a *thin* S3-compatible wrapper around boto3, using
``asyncio.to_thread`` to keep blocking IO off the event loop. R2 speaks
the S3 API so the same code works against MinIO / Backblaze if we ever
need to migrate.

Configuration
-------------
All four of ``r2_endpoint``, ``r2_bucket``, ``r2_access_key_id``,
``r2_secret_access_key`` must be set for the client to be enabled.
If any is blank the factory returns ``None`` and callers short-circuit
the upload path — useful for local dev (no R2) and the tests.

Idempotency
-----------
``put_tile`` overwrites existing keys; ``object_exists`` lets callers
short-circuit a render they've already uploaded. NATS delivers grid
events at-least-once, so duplicate prewarms are routine.

Failure model
-------------
``put_tile`` returns ``None`` on success, raises on transport / auth
failure — the prewarm consumer catches and logs, same shape as the
existing per-tile render-failure path. ``delete_grid`` is best-effort
(missing keys count as success); orphan objects are mopped up by a
bucket lifecycle rule as a backstop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Literal

import boto3
import structlog
from botocore.client import Config

from aeroza.config import Settings, get_settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    from aeroza.tiles.raster import TileFormat

log = structlog.get_logger(__name__)

# Public so the prewarm + retention modules can read it without
# importing botocore. Picked deliberately for stale-tolerant tile bytes:
# pinned tile content for a ``(file_key, z, x, y, format)`` tuple never
# changes, so 1-year immutable is the right Cache-Control.
TILE_CACHE_CONTROL: Final[str] = "public, max-age=31536000, immutable"

# Object key prefix. Kept top-level (no ``tiles/`` prefix on top of the
# bucket) because the bucket itself only stores tiles — separation of
# concerns by *bucket* not by prefix. Same convention as the source
# layout: ``CONUS/MergedReflectivity_…/2026…/MRMS_…grib2.gz``.
TILE_KEY_TEMPLATE: Final[str] = "{file_key}/{z}/{x}/{y}.{format}"

# Pagination cap for the list-then-delete pass in ``delete_grid``. R2's
# ListObjectsV2 returns up to 1000 keys per call; we set the page size
# to match. A CONUS grid pyramid at z=2..8 is ~680 tiles × 2 formats =
# ~1360 keys, so most grids fit in two pages.
_LIST_PAGE_SIZE: Final[int] = 1000


def _content_type_for(fmt: TileFormat | str) -> str:
    if fmt == "webp":
        return "image/webp"
    if fmt == "png":
        return "image/png"
    raise ValueError(f"unsupported tile format: {fmt!r}")


def _object_key(*, file_key: str, z: int, x: int, y: int, fmt: TileFormat | str) -> str:
    return TILE_KEY_TEMPLATE.format(file_key=file_key, z=z, x=x, y=y, format=fmt)


@dataclass(frozen=True, slots=True)
class R2Client:
    """Thin S3-compatible client bound to a single Cloudflare R2 bucket.

    Methods are async; under the hood they offload boto3's blocking
    calls to a worker thread via :func:`asyncio.to_thread`. boto3
    sessions are thread-safe per the docs, and we keep a single client
    instance for the life of the process.
    """

    bucket: str
    endpoint: str
    _client: Any

    async def put_tile(
        self,
        *,
        file_key: str,
        z: int,
        x: int,
        y: int,
        fmt: TileFormat | str,
        body: bytes,
    ) -> None:
        """Upload ``body`` to ``{file_key}/{z}/{x}/{y}.{fmt}``.

        Overwrites silently — duplicate prewarms (NATS at-least-once
        delivery, manual re-runs) converge to the same end state.
        Sets ``Cache-Control: public, max-age=31536000, immutable``
        so the Cloudflare edge can cache the response forever.
        """
        key = _object_key(file_key=file_key, z=z, x=x, y=y, fmt=fmt)
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=_content_type_for(fmt),
            CacheControl=TILE_CACHE_CONTROL,
        )

    async def object_exists(
        self,
        *,
        file_key: str,
        z: int,
        x: int,
        y: int,
        fmt: TileFormat | str,
    ) -> bool:
        """Return ``True`` if the object already exists in R2.

        Used by the prewarm consumer to skip already-uploaded tiles on
        a retried event. A HEAD request is cheap (Class B op, free
        tier covers 10M/month) and saves both the render CPU and the
        Class A op of a redundant PUT.
        """
        key = _object_key(file_key=file_key, z=z, x=x, y=y, fmt=fmt)
        try:
            await asyncio.to_thread(self._client.head_object, Bucket=self.bucket, Key=key)
        except self._client.exceptions.ClientError as exc:
            # botocore raises a generic ClientError with a 404 status
            # code for "not found" — there is no dedicated NoSuchKey
            # exception class on the head_object surface.
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    async def delete_grid(self, *, file_key: str) -> int:
        """Delete every object under the ``{file_key}/`` prefix.

        Best-effort: a missing key is a successful delete from the
        caller's perspective (the grid is gone either way). Returns
        the count of objects removed, for structured logging.

        Pages through ListObjectsV2 + DeleteObjects in 1000-key
        chunks. A typical CONUS grid pyramid is ~1.4k keys (z=2..8 ×
        ~680 tiles × 1-2 formats), so this completes in 1-2 round
        trips.
        """
        deleted = 0
        continuation: str | None = None
        while True:
            list_kwargs: dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": f"{file_key}/",
                "MaxKeys": _LIST_PAGE_SIZE,
            }
            if continuation is not None:
                list_kwargs["ContinuationToken"] = continuation
            page = await asyncio.to_thread(self._client.list_objects_v2, **list_kwargs)

            keys = [obj["Key"] for obj in page.get("Contents", [])]
            if keys:
                await asyncio.to_thread(
                    self._client.delete_objects,
                    Bucket=self.bucket,
                    Delete={
                        "Objects": [{"Key": k} for k in keys],
                        "Quiet": True,
                    },
                )
                deleted += len(keys)

            if not page.get("IsTruncated"):
                break
            continuation = page.get("NextContinuationToken")
            if continuation is None:
                break

        return deleted

    def public_url(
        self,
        *,
        base_url: str,
        file_key: str,
        z: int,
        x: int,
        y: int,
        fmt: TileFormat | str,
    ) -> str:
        """Build the public CDN URL for a tile.

        ``base_url`` should be the custom-domain origin
        (``https://tiles.aeroza.app``) — the bucket endpoint itself
        isn't intended for public reads. The path mirrors the object
        key 1:1.
        """
        key = _object_key(file_key=file_key, z=z, x=x, y=y, fmt=fmt)
        return f"{base_url.rstrip('/')}/{key}"


def build_r2_client(settings: Settings) -> R2Client | None:
    """Construct an :class:`R2Client` if all R2 settings are configured.

    Returns ``None`` when any of the four required env vars is blank —
    callers should treat the return value as "R2 disabled, fall back to
    the legacy on-demand render path". Lets local dev and the test
    suite operate without an R2 dependency.
    """
    endpoint = (settings.r2_endpoint or "").strip()
    bucket = (settings.r2_bucket or "").strip()
    access_key = (settings.r2_access_key_id or "").strip()
    secret_key = (settings.r2_secret_access_key or "").strip()
    if not (endpoint and bucket and access_key and secret_key):
        return None

    # ``signature_version="s3v4"`` is required for R2; the default v2
    # signer in older boto3 chains will 403 on every request.
    # ``region_name="auto"`` matches Cloudflare's documented value;
    # the SDK requires *some* region string even though R2 is
    # globally distributed.
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
    )
    return R2Client(bucket=bucket, endpoint=endpoint, _client=client)


@lru_cache(maxsize=1)
def get_default_r2_client() -> R2Client | None:
    """Process-scoped singleton, built from :func:`get_settings`.

    Cached so the prewarm subscriber + retention worker share one
    underlying boto3 client (boto3 clients are thread-safe and
    connection-pool-aware, so reuse is the right default).
    """
    return build_r2_client(get_settings())


def reset_default_r2_client() -> None:
    """Test seam — clear the cached singleton so a fresh settings
    snapshot can take effect (e.g. ``monkeypatch.setenv("AEROZA_R2_*")``
    in a per-test fixture)."""
    get_default_r2_client.cache_clear()


# Re-exported so callers don't need to import Literal/TileFormat to
# annotate ``fmt`` parameters — keeps the public surface narrow.
R2TileFormat = Literal["webp", "png"]


__all__ = [
    "TILE_CACHE_CONTROL",
    "TILE_KEY_TEMPLATE",
    "R2Client",
    "R2TileFormat",
    "build_r2_client",
    "get_default_r2_client",
    "reset_default_r2_client",
]


def _validate_fmts(fmts: Iterable[TileFormat | str]) -> tuple[str, ...]:
    """Public-ish helper used by the prewarm module to canonicalise the
    set of formats it'll upload. Defined here rather than at the
    callsite so the supported-format guardrail lives next to
    ``_content_type_for``."""
    out: list[str] = []
    for fmt in fmts:
        _content_type_for(fmt)  # raises on unsupported
        out.append(str(fmt))
    return tuple(out)
