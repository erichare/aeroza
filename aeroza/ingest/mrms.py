"""MRMS (Multi-Radar / Multi-Sensor) file discovery on AWS Open Data.

This slice covers **catalog** only — listing files in the public
``noaa-mrms-pds`` bucket and parsing their object keys into a structured
:class:`MrmsFile` record. Downloading the GRIB2 payload, decoding it, and
materialising it as Zarr is the next slice; that work needs the eccodes
system library and a much larger surface, so we ship discovery first
because it's independently useful (a "what's available right now" feed)
and unblocks every later piece.

Bucket layout
-------------
NOAA publishes MRMS at::

    s3://noaa-mrms-pds/CONUS/<Product>_<Level>/<YYYYMMDD>/MRMS_<Product>_<Level>_<YYYYMMDD>-<HHMMSS>.grib2.gz

So a key like::

    CONUS/MergedReflectivityComposite_00.50/20260501/MRMS_MergedReflectivityComposite_00.50_20260501-122000.grib2.gz

decodes to product ``MergedReflectivityComposite``, level ``00.50``,
valid at ``2026-05-01T12:20:00Z``. (MRMS file timestamps are UTC.)
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import structlog

from aeroza.ingest._aws import open_data_s3_client

log = structlog.get_logger(__name__)

MRMS_BUCKET: Final[str] = "noaa-mrms-pds"
MRMS_PREFIX_ROOT: Final[str] = "CONUS"

# MRMS_<Product>_<Level>_<YYYYMMDD>-<HHMMSS>.grib2(.gz)
# Product tokens may contain alphanumerics and underscores; level is e.g.
# "00.50" or "00.00" (sometimes just "00"); the suffix is always ".grib2"
# possibly followed by ".gz".
_KEY_RE: Final[re.Pattern[str]] = re.compile(
    r"""
    ^CONUS/
    (?P<product>[A-Za-z0-9]+)_
    (?P<level>\d{2}(?:\.\d{2})?)/
    \d{8}/
    MRMS_(?P=product)_(?P=level)_
    (?P<date>\d{8})-(?P<time>\d{6})
    \.grib2(?:\.gz)?
    $
    """,
    re.VERBOSE,
)


@dataclass(frozen=True, slots=True)
class MrmsFile:
    """One MRMS object on S3, identified by its key.

    ``valid_at`` is timezone-aware (UTC). ``size_bytes`` and ``etag`` are
    populated from the listing response so callers can detect changes.
    """

    key: str
    product: str
    level: str
    valid_at: datetime
    size_bytes: int
    etag: str | None

    @property
    def product_level(self) -> str:
        """The combined ``"<product>_<level>"`` token used in S3 prefixes."""
        return f"{self.product}_{self.level}"


def parse_mrms_key(key: str) -> MrmsFile | None:
    """Decode a single S3 object key into a :class:`MrmsFile`, or return ``None``.

    Returns ``None`` for keys that don't match the expected MRMS shape
    (e.g. directory placeholders, unrelated files). ``size_bytes`` defaults
    to ``0`` and ``etag`` to ``None`` since the key alone doesn't carry
    those — callers building a record from a list-objects response should
    populate them from the response's ``Size`` and ``ETag`` fields instead.
    """
    match = _KEY_RE.match(key)
    if match is None:
        return None
    date_part = match["date"]
    time_part = match["time"]
    try:
        valid_at = datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
    return MrmsFile(
        key=key,
        product=match["product"],
        level=match["level"],
        valid_at=valid_at,
        size_bytes=0,
        etag=None,
    )


def _prefix_for(product: str, level: str, day: datetime | None = None) -> str:
    """Build the S3 prefix that scopes a list-objects call to one product."""
    base = f"{MRMS_PREFIX_ROOT}/{product}_{level}/"
    if day is None:
        return base
    return base + day.strftime("%Y%m%d") + "/"


async def list_mrms_files(
    *,
    product: str,
    level: str,
    day: datetime | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    s3_client: Any = None,
    bucket: str = MRMS_BUCKET,
) -> tuple[MrmsFile, ...]:
    """Return MRMS files for ``product``/``level``, filtered to a time window.

    Args:
        product: e.g. ``"MergedReflectivityComposite"``.
        level: e.g. ``"00.50"``. Must match the bucket layout exactly.
        day: When set, restrict to that single UTC day; otherwise list across
            all days in the bucket (slow — only useful with ``since``).
        since: Inclusive lower bound on ``valid_at``.
        until: Exclusive upper bound on ``valid_at``.
        s3_client: Inject a configured boto3 S3 client (e.g. from
            :func:`aeroza.ingest._aws.open_data_s3_client`). When ``None``,
            a one-shot anonymous-reads client is created on the fly.
        bucket: Override the source bucket — primarily for tests.

    Returns:
        Files in chronological order (earliest ``valid_at`` first), keyed by
        ``key``. Malformed object keys are logged and skipped.
    """
    client = s3_client if s3_client is not None else open_data_s3_client()
    prefix = _prefix_for(product, level, day)

    raw_objects = await asyncio.to_thread(_list_all_objects, client, bucket, prefix)
    files: list[MrmsFile] = []
    skipped = 0
    for obj in raw_objects:
        parsed = parse_mrms_key(obj["Key"])
        if parsed is None:
            skipped += 1
            continue
        if since is not None and parsed.valid_at < since:
            continue
        if until is not None and parsed.valid_at >= until:
            continue
        files.append(
            MrmsFile(
                key=parsed.key,
                product=parsed.product,
                level=parsed.level,
                valid_at=parsed.valid_at,
                size_bytes=int(obj.get("Size", 0)),
                etag=_strip_etag(obj.get("ETag")),
            )
        )
    if skipped:
        log.info("mrms.list.skipped_unparseable", count=skipped, prefix=prefix)
    files.sort(key=lambda f: f.valid_at)
    return tuple(files)


def _list_all_objects(s3_client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """Paginated ``list_objects_v2`` collected into one list. Synchronous."""
    paginator = s3_client.get_paginator("list_objects_v2")
    out: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def _strip_etag(raw: str | None) -> str | None:
    """boto3 returns ETags wrapped in double quotes — peel them off."""
    if raw is None:
        return None
    return raw.strip('"') or None


def keys(files: Iterable[MrmsFile]) -> tuple[str, ...]:
    """Return the S3 keys of ``files`` as a tuple. Convenience for callers."""
    return tuple(f.key for f in files)
