#!/usr/bin/env python3
"""One-shot prewarm backfill — seed R2 with the most-recent N grids.

Why this exists:

The ``prewarm`` worker (``aeroza-prewarm-tiles``) only renders tiles
for grids whose ``aeroza.mrms.grids.new`` event arrives *after* the
worker is up. Right after a fresh deploy — or after the worker has
been down — R2 is missing every historical grid the catalog still
points at, so the radar's timeline scrubber 404s on every old
fileKey. The on-demand write-through (PR #94) eventually fills these
on first visit, but until then page-loads see a sea of blank tiles.

This script seeds R2 directly: it pulls the latest N rows from
``/v1/mrms/files``, then walks the CONUS z=2..8 pyramid by hitting
the FastAPI cold-render route
``/v1/mrms/tiles/{z}/{x}/{y}.png?fileKey=…``. Each request renders a
tile *and* fires the write-through upload — so the script's only job
is to make the requests; R2 fills as a side effect.

Run after the first prod deploy (or any time R2 has been wiped):

    AEROZA_API_URL=https://api.aeroza.app \\
        python scripts/backfill-prewarm.py --count 12

Per fileKey: ~680 tiles × 1-6s render time / 32-way concurrency ≈
2-4 minutes wall clock. With 12 fileKeys (~24min back of cache) the
total backfill is under an hour.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

import httpx

# CONUS bbox + zooms must mirror aeroza/tiles/prewarm.py so the
# backfill walks exactly the tiles the live prewarm worker would,
# nothing more, nothing less. Drift here means the script either
# misses tiles the worker would have published, or wastes requests
# rendering tiles the worker would skip.
CONUS_BBOX: Final[tuple[float, float, float, float]] = (-125.0, 24.0, -66.0, 50.0)
DEFAULT_ZOOMS: Final[tuple[int, ...]] = (2, 3, 4, 5, 6, 7, 8)
DEFAULT_COUNT: Final[int] = 6
DEFAULT_CONCURRENCY: Final[int] = 16
DEFAULT_TIMEOUT_S: Final[float] = 30.0


@dataclass(frozen=True, slots=True)
class BackfillStats:
    requested: int
    ok: int
    failed: int


def conus_tile_coords(zoom: int) -> tuple[tuple[int, int], ...]:
    """CONUS tile coords at ``zoom`` — same math as aeroza.tiles.prewarm."""
    if zoom < 0:
        raise ValueError(f"zoom must be non-negative, got {zoom}")
    n = 1 << zoom
    lng_w, lat_s, lng_e, lat_n = CONUS_BBOX

    def lng_to_x(lng: float) -> int:
        return int((lng + 180.0) / 360.0 * n)

    def lat_to_y(lat: float) -> int:
        clamped = max(-85.0511, min(85.0511, lat))
        rad = math.radians(clamped)
        return int((1.0 - math.log(math.tan(rad) + 1.0 / math.cos(rad)) / math.pi) / 2.0 * n)

    x_min = max(0, min(n - 1, lng_to_x(lng_w)))
    x_max = max(0, min(n - 1, lng_to_x(lng_e)))
    y_min = max(0, min(n - 1, lat_to_y(lat_n)))
    y_max = max(0, min(n - 1, lat_to_y(lat_s)))

    return tuple((x, y) for x in range(x_min, x_max + 1) for y in range(y_min, y_max + 1))


async def list_recent_file_keys(
    client: httpx.AsyncClient,
    *,
    api_url: str,
    count: int,
    product: str,
    level: str,
) -> list[str]:
    """Pull the most-recent ``count`` materialised-grid fileKeys.

    Hits ``/v1/mrms/grids`` rather than ``/v1/mrms/files`` because
    only materialised grids can be tile-rendered — querying the file
    catalog directly would queue up tiles for files that don't have a
    Zarr yet (cold-render route returns 404).
    """
    resp = await client.get(
        f"{api_url.rstrip('/')}/v1/mrms/grids",
        params={"product": product, "level": level, "limit": count},
    )
    resp.raise_for_status()
    payload = resp.json()
    items = payload.get("items", [])
    # camelCase per the API serialisation; fall back to snake_case
    # in case a future schema change introduces both shapes.
    keys = [it.get("fileKey") or it.get("file_key") for it in items]
    return [k for k in keys if k]


async def warm_tile(
    client: httpx.AsyncClient,
    *,
    api_url: str,
    file_key: str,
    z: int,
    x: int,
    y: int,
    semaphore: asyncio.Semaphore,
) -> bool:
    """GET one tile so the FastAPI cold-render path writes it to R2.

    Returns True on 2xx. Network errors and non-2xx are swallowed and
    counted as failures — the backfill is best-effort.
    """
    url = f"{api_url.rstrip('/')}/v1/mrms/tiles/{z}/{x}/{y}.png"
    params = {"fileKey": file_key}
    # ``Accept: image/webp`` so the route negotiates to WebP and
    # populates the .webp key in R2 (the only format the prewarm
    # worker uploads — staying parallel keeps the eventual prewarm
    # event a no-op rather than re-uploading PNG too).
    headers = {"Accept": "image/webp"}
    async with semaphore:
        try:
            resp = await client.get(url, params=params, headers=headers)
        except httpx.HTTPError:
            return False
    return 200 <= resp.status_code < 300


async def backfill_one_grid(
    client: httpx.AsyncClient,
    *,
    api_url: str,
    file_key: str,
    zooms: Iterable[int],
    semaphore: asyncio.Semaphore,
) -> BackfillStats:
    """Walk the full CONUS pyramid for one fileKey."""
    requested = 0
    ok = 0
    failed = 0
    tasks: list[asyncio.Task[bool]] = []
    for zoom in zooms:
        for x, y in conus_tile_coords(zoom):
            tasks.append(
                asyncio.create_task(
                    warm_tile(
                        client,
                        api_url=api_url,
                        file_key=file_key,
                        z=zoom,
                        x=x,
                        y=y,
                        semaphore=semaphore,
                    )
                )
            )
            requested += 1

    for result in await asyncio.gather(*tasks):
        if result:
            ok += 1
        else:
            failed += 1
    return BackfillStats(requested=requested, ok=ok, failed=failed)


async def run(args: argparse.Namespace) -> int:
    api_url = args.api_url or os.environ.get("AEROZA_API_URL")
    if not api_url:
        print(
            "error: --api-url not provided and AEROZA_API_URL is unset",
            file=sys.stderr,
        )
        return 2

    semaphore = asyncio.Semaphore(args.concurrency)
    timeout = httpx.Timeout(args.timeout)
    async with httpx.AsyncClient(timeout=timeout) as client:
        file_keys = await list_recent_file_keys(
            client,
            api_url=api_url,
            count=args.count,
            product=args.product,
            level=args.level,
        )
        if not file_keys:
            print(
                "[backfill-prewarm] no materialised grids returned — "
                "either the API is reachable but the catalog is empty, "
                "or the materialiser hasn't caught up yet.",
                file=sys.stderr,
            )
            return 1

        print(
            f"[backfill-prewarm] seeding R2 via {api_url} "
            f"for {len(file_keys)} fileKey(s), zooms={list(args.zooms)}, "
            f"concurrency={args.concurrency}",
            file=sys.stderr,
        )

        totals = BackfillStats(requested=0, ok=0, failed=0)
        for idx, file_key in enumerate(file_keys, start=1):
            stats = await backfill_one_grid(
                client,
                api_url=api_url,
                file_key=file_key,
                zooms=args.zooms,
                semaphore=semaphore,
            )
            totals = BackfillStats(
                requested=totals.requested + stats.requested,
                ok=totals.ok + stats.ok,
                failed=totals.failed + stats.failed,
            )
            print(
                f"[backfill-prewarm] {idx}/{len(file_keys)} {file_key} "
                f"requested={stats.requested} ok={stats.ok} failed={stats.failed}",
                file=sys.stderr,
            )

        print(
            f"[backfill-prewarm] done — requested={totals.requested} "
            f"ok={totals.ok} failed={totals.failed}",
            file=sys.stderr,
        )

    return 0 if totals.failed == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill-prewarm",
        description=(
            "Walk the CONUS tile pyramid for the latest N materialised "
            "grids, hitting the FastAPI cold-render route so each tile "
            "lands in R2 via the write-through path. One-shot script — "
            "use after a fresh deploy or after R2 has been wiped."
        ),
    )
    parser.add_argument(
        "--api-url",
        default=None,
        help=(
            "Base URL of the deployed API (e.g. https://api.aeroza.app). "
            "Defaults to $AEROZA_API_URL."
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=(
            f"Number of recent fileKeys to backfill (default: {DEFAULT_COUNT}). "
            "Each fileKey is ~680 tiles."
        ),
    )
    parser.add_argument(
        "--product",
        default="MergedReflectivityComposite",
        help="MRMS product (default: MergedReflectivityComposite).",
    )
    parser.add_argument(
        "--level",
        default="00.50",
        help="MRMS product level (default: 00.50).",
    )
    parser.add_argument(
        "--zooms",
        type=int,
        nargs="+",
        default=list(DEFAULT_ZOOMS),
        help=(
            "Zoom levels to walk "
            f"(default: {' '.join(map(str, DEFAULT_ZOOMS))}). "
            "Must match the prewarm worker's zoom range or R2 ends up "
            "with an inconsistent pyramid."
        ),
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=(
            f"Max concurrent in-flight requests (default: {DEFAULT_CONCURRENCY}). "
            "Tune down if the API is rate-limited or memory-constrained."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=(
            f"Per-request HTTP timeout in seconds (default: {DEFAULT_TIMEOUT_S}). "
            "Cold renders at z=8 can take a few seconds, so don't go below 15."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
