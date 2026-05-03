"""End-to-end tests for ``GET /v1/mrms/tiles/{z}/{x}/{y}.png``.

The route opens the latest matching Zarr grid and renders an XYZ tile.
We seed a synthetic grid covering a known lat/lng box and assert:

1. A tile that overlaps the grid extent has visible (non-zero alpha)
   pixels.
2. A tile entirely outside the grid extent is fully transparent (alpha=0).
3. Without any grid materialised, the route falls back to a transparent
   tile (200 OK with image/png) so MapLibre doesn't spam 404s.
4. Cache-Control + grid-key headers are set so clients can verify which
   source populated a tile.
5. ``fileKey`` query pins a specific grid even if a newer one exists.
"""

from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from httpx import AsyncClient
from PIL import Image
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

PRODUCT = "MergedReflectivityComposite"
LEVEL = "00.50"

# Synthetic grid covering roughly the central US so a small handful of
# zoom-3 / zoom-4 tiles intersect it.
LAT_AXIS = np.linspace(50.0, 25.0, 32)  # north → south, MRMS-style
LNG_AXIS = np.linspace(-110.0, -80.0, 32)  # west → east


def _write_grid(target: Path) -> str:
    """A 32x32 grid with reflectivity = 30 dBZ in the centre 16x16, NaN
    elsewhere. Enough variation to verify alpha=0 on transparent cells
    and alpha>0 on filled cells."""
    values = np.full((32, 32), np.nan, dtype=np.float32)
    values[8:24, 8:24] = 30.0
    da = xr.DataArray(
        values,
        coords={"latitude": LAT_AXIS, "longitude": LNG_AXIS},
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


def _file(key: str, valid_at: datetime) -> MrmsFile:
    return MrmsFile(
        key=key,
        product=PRODUCT,
        level=LEVEL,
        valid_at=valid_at,
        size_bytes=1_000,
        etag="e",
    )


def _locator(file_key: str, zarr_uri: str) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(32, 32),
        dtype="float32",
        nbytes=32 * 32 * 4,
    )


async def _seed(
    integration_db: Database,
    files: tuple[MrmsFile, ...],
    locators: tuple[MrmsGridLocator, ...],
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(session, files)
        for loc in locators:
            await upsert_mrms_grid(session, loc)
        await session.commit()


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> object:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


def _decode_alpha(png: bytes) -> np.ndarray:
    img = Image.open(BytesIO(png)).convert("RGBA")
    return np.asarray(img)[..., 3]


async def test_tile_over_grid_has_visible_pixels(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # z=3 / x=2 / y=3 covers (-90, -45) lng × (40.98, 0) lat — overlaps
    # the centre of our (-110..-80, 25..50) grid.
    response = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.headers["x-aeroza-grid-key"] == "k1"
    assert "x-aeroza-grid-valid-at" in response.headers
    assert "max-age=" in response.headers["cache-control"]

    alpha = _decode_alpha(response.content)
    assert alpha.shape == (256, 256)
    assert (alpha > 0).any(), "some pixels should be opaque inside the grid"


async def test_tile_outside_grid_is_transparent(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # z=3 / x=6 / y=3 is east of -45° W — fully outside the CONUS grid.
    response = await api_client.get("/v1/mrms/tiles/3/6/3.png")
    assert response.status_code == 200
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()


async def test_no_grid_falls_back_to_transparent(api_client: AsyncClient) -> None:
    # No catalog rows seeded; route should return a transparent tile, not
    # a 404, so MapLibre doesn't aggressively retry.
    response = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert "x-aeroza-grid-key" not in response.headers
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()


async def test_file_key_pins_specific_grid(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    older_uri = _write_grid(tmp_path / "older.zarr")
    newer_path = tmp_path / "newer.zarr"
    # Newer grid: same shape, different fill value (50 dBZ — visibly
    # different colour on the ramp).
    da = xr.DataArray(
        np.full((32, 32), 50.0, dtype=np.float32),
        coords={"latitude": LAT_AXIS, "longitude": LNG_AXIS},
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    da.to_zarr(str(newer_path), mode="w")
    older_file = _file("older", datetime(2026, 5, 1, 11, 0, tzinfo=UTC))
    newer_file = _file("newer", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(
        integration_db,
        (older_file, newer_file),
        (_locator(older_file.key, older_uri), _locator(newer_file.key, str(newer_path))),
    )

    # No fileKey → newer grid populates the tile (we expect 50 dBZ colour).
    latest = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert latest.headers["x-aeroza-grid-key"] == "newer"

    # fileKey pins → older grid wins even though newer exists.
    pinned = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=older")
    assert pinned.headers["x-aeroza-grid-key"] == "older"


async def test_zoom_clamp(api_client: AsyncClient) -> None:
    response = await api_client.get("/v1/mrms/tiles/30/0/0.png")
    assert response.status_code == 422  # FastAPI Path validator


async def test_xy_out_of_range_is_400(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # z=2 → max x/y = 3
    response = await api_client.get("/v1/mrms/tiles/2/4/0.png")
    assert response.status_code == 400


async def test_unknown_file_key_falls_back_to_transparent(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """Pinning a file_key that doesn't exist returns transparent (same
    behaviour as no-grid case) instead of 404 — keeps the MapLibre
    retry loop quiet."""
    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=does-not-exist")
    assert response.status_code == 200
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()


async def test_pinned_tile_emits_immutable_cache_header_and_caches_server_side(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """When the request pins a fileKey, the response is forever-immutable
    (the source grid never changes), so:

    * ``Cache-Control: public, max-age=31536000, immutable`` — lets
      browsers + CDNs hold the tile across the radar replay loop.
    * The first request renders + populates the LRU
      (``X-Aeroza-Tile-Cache: miss``).
    * The second request hits the LRU
      (``X-Aeroza-Tile-Cache: hit``) and the bytes are byte-identical.
    """
    from aeroza.tiles.cache import TilePngCache, set_default_cache

    set_default_cache(TilePngCache(max_bytes=1024 * 1024))

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("pinned-key", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    first = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=pinned-key")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert first.headers["x-aeroza-tile-cache"] == "miss"

    second = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=pinned-key")
    assert second.status_code == 200
    assert second.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert second.headers["x-aeroza-tile-cache"] == "hit"
    # Bytes are deterministic — the LRU returns the same render the
    # first request produced.
    assert second.content == first.content


async def test_live_tile_keeps_short_ttl_and_bypasses_lru(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """Live-mode tiles (no fileKey) still get the short TTL and are
    deliberately not server-cached — the latest grid moves with new
    ingests, so caching the rendered bytes by zoom/coords would race
    the materialiser."""
    from aeroza.tiles.cache import TilePngCache, set_default_cache

    set_default_cache(TilePngCache(max_bytes=1024 * 1024))

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert response.status_code == 200
    assert "max-age=60" in response.headers["cache-control"]
    assert "immutable" not in response.headers["cache-control"]
    assert response.headers["x-aeroza-tile-cache"] == "bypass"


async def test_accept_image_webp_returns_webp_with_vary_header(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """When the client accepts ``image/webp``, the response is WebP-encoded
    with ``Content-Type: image/webp`` and ``Vary: Accept`` so shared caches
    partition correctly between PNG and WebP variants. The URL still
    ends in ``.png`` for MapLibre's tile-template compatibility — the
    extension lies, but every modern browser sends the right Accept."""
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=k1",
        headers={"Accept": "image/webp,image/png,*/*"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/webp"
    assert response.headers["vary"] == "Accept"
    # Sanity-check the actual bytes: WebP files start with the RIFF
    # magic and carry "WEBP" in bytes 8-12.
    assert response.content[:4] == b"RIFF"
    assert response.content[8:12] == b"WEBP"


async def test_png_and_webp_caches_are_disjoint_per_format(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """The LRU keys on ``(file_key, z, x, y, format)`` so a request for
    PNG doesn't accidentally serve cached WebP bytes (or vice versa).
    First request in each format is a miss; second is a hit."""
    from aeroza.tiles.cache import TilePngCache, set_default_cache

    set_default_cache(TilePngCache(max_bytes=1024 * 1024))

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    png_first = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=k1",
        headers={"Accept": "image/png"},
    )
    assert png_first.headers["x-aeroza-tile-cache"] == "miss"
    assert png_first.headers["content-type"] == "image/png"

    webp_first = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=k1",
        headers={"Accept": "image/webp"},
    )
    # Cross-format request: must be a miss because PNG cached above
    # is in a separate slot.
    assert webp_first.headers["x-aeroza-tile-cache"] == "miss"
    assert webp_first.headers["content-type"] == "image/webp"

    png_second = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=k1",
        headers={"Accept": "image/png"},
    )
    assert png_second.headers["x-aeroza-tile-cache"] == "hit"
    assert png_second.content == png_first.content
