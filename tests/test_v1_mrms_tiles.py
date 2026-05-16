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

import asyncio
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
    # the centre of our (-110..-80, 25..50) grid. Explicit
    # ``Accept: image/png`` keeps this assertion stable now that
    # ``Accept: */*`` (httpx default) negotiates to WebP — the PNG
    # path needs an opt-in signal.
    response = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png",
        headers={"Accept": "image/png"},
    )
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
    response = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png",
        headers={"Accept": "image/png"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert "x-aeroza-grid-key" not in response.headers
    # The transparent fallback uses the same live cache directive as a
    # rendered live tile — short freshness + stale-while-revalidate
    # window — so MapLibre / iOS URLCache stop hammering the route when
    # a grid is briefly missing during materialisation.
    assert "max-age=60" in response.headers["cache-control"]
    assert "stale-while-revalidate=120" in response.headers["cache-control"]
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


async def test_render_failure_falls_back_to_transparent_with_live_ttl(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Zarr backing a cataloged grid is unreachable from the
    renderer — retention race, Railway volume wipe, transient IO blip —
    the route serves a transparent tile + the live TTL instead of 500.

    The radar loop's perspective is what matters: one blank frame
    every few minutes is recoverable; a 500 spike that taints the
    browser's MapLibre raster source is not. The live TTL (not the
    immutable pinned header) is the other half — we deliberately
    don't memoise this absence forever because the next materialiser
    pass or deploy can fix the underlying storage.
    """
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("ghost", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # Simulate the storage being gone underneath us. We monkeypatch the
    # renderer (rather than rm-rf'ing the directory) because xarray's
    # exact exception class for a missing zarr varies by version /
    # backend, but the route's handler is keyed on the
    # FileNotFoundError / OSError family that production has actually
    # surfaced — pinning the test to that contract.
    def _explode(**_kw: object) -> bytes:
        raise FileNotFoundError("simulated zarr deletion mid-request")

    monkeypatch.setattr("aeroza.query.v1.mrms.render_tile_image", _explode)

    response = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=ghost",
        headers={"Accept": "image/png"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    # Live TTL + stale-while-revalidate, NOT the immutable pinned
    # header — clients must re-check, because the absence is
    # transient.
    assert "max-age=60" in response.headers["cache-control"]
    assert "stale-while-revalidate=120" in response.headers["cache-control"]
    assert "immutable" not in response.headers["cache-control"]
    # No grid-key header — the helper produces a generic transparent
    # response, identical in shape to the "no grid materialised" path.
    assert "x-aeroza-grid-key" not in response.headers
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()


async def test_oserror_also_falls_back_to_transparent(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OSError (generic IO failure on the zarr backend — permission
    denied, broken pipe, object-store 503, etc.) takes the same
    transparent-tile fallback path as FileNotFoundError. We don't want
    a transient backend hiccup to surface as a 500 spike either.
    """
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("io-error", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    def _explode(**_kw: object) -> bytes:
        raise OSError("simulated backend IO failure")

    monkeypatch.setattr("aeroza.query.v1.mrms.render_tile_image", _explode)

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=io-error")
    assert response.status_code == 200
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()


async def test_arbitrary_exception_in_renderer_falls_back_to_transparent(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The route's catch-all swallows any unexpected exception class.

    The radar loop hammers tiles continuously; a single 500 spike taints
    MapLibre's raster source and breaks the radar UI until the page
    reloads. So we don't want to be picky about which exception
    classes deserve a transparent fallback — anything that's not an
    intentional HTTPException becomes a blank frame plus a structured
    warning log. Production-observed culprits we'd otherwise leak as
    500s include SQLAlchemy pool exhaustion, numpy/Pillow MemoryError
    under burst, and transient backend exceptions we haven't catalogued
    yet.
    """
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("surprise", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    def _explode(**_kw: object) -> bytes:
        # RuntimeError is the most "this isn't IO and isn't a known
        # class" signal we have — proves the catch is by exception
        # base, not by a hand-curated allowlist.
        raise RuntimeError("totally unexpected render-time failure")

    monkeypatch.setattr("aeroza.query.v1.mrms.render_tile_image", _explode)

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=surprise")
    assert response.status_code == 200
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()
    assert "max-age=60" in response.headers["cache-control"]
    assert "immutable" not in response.headers["cache-control"]


async def test_db_lookup_failure_falls_back_to_transparent(
    api_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLAlchemy raising during the catalog lookup is the most likely
    real-world cause of 500s on a small Railway dyno: a burst of 60+
    radar-loop tiles can saturate ``pool_size=5 + max_overflow=5``
    when each session is held through the 1-6s render. We want pool
    exhaustion (and any other DBAPIError) to surface as a blank frame
    via the route's outer catch-all, not a 500.
    """

    async def _explode(*_args: object, **_kw: object) -> object:
        # Generic Exception — proves the outer catch isn't keyed on a
        # specific SQLAlchemy class. The actual production error
        # surface includes ``TimeoutError``, ``OperationalError``,
        # ``DBAPIError``, etc.
        raise RuntimeError("simulated DB failure during grid lookup")

    monkeypatch.setattr("aeroza.query.v1.mrms.find_mrms_grid_by_key", _explode)

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=anything")
    assert response.status_code == 200
    alpha = _decode_alpha(response.content)
    assert (alpha == 0).all()
    assert "max-age=60" in response.headers["cache-control"]


async def test_keyerror_still_502s_for_structural_grid_bug(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KeyError stays a 502: it signals the variable isn't in the Zarr
    store, which is a structural data bug (renderer expectation vs
    materialised grid mismatch), not a transient storage hiccup. The
    caller should know the upstream data is wrong so it can be fixed
    rather than papering over with a transparent tile.
    """
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("schema-bug", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    def _explode(**_kw: object) -> bytes:
        raise KeyError("variable 'reflectivity' not in store")

    monkeypatch.setattr("aeroza.query.v1.mrms.render_tile_image", _explode)

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=schema-bug")
    assert response.status_code == 502


async def test_cold_render_writes_through_to_r2(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every cold render fires off a fire-and-forget R2 upload so the
    static CDN origin self-heals as the radar UI exercises the
    fallback. When the prewarm subscriber eventually catches up the
    path is a no-op (R2 already has the bytes); until then it's the
    *only* way tiles end up at ``tiles.aeroza.app`` if NATS event
    delivery is broken.
    """
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("write-through", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    uploads: list[dict[str, object]] = []

    class _FakeR2:
        async def put_tile(
            self,
            *,
            file_key: str,
            z: int,
            x: int,
            y: int,
            fmt: str,
            body: bytes,
        ) -> None:
            uploads.append(
                {
                    "file_key": file_key,
                    "z": z,
                    "x": x,
                    "y": y,
                    "fmt": fmt,
                    "nbytes": len(body),
                }
            )

    monkeypatch.setattr("aeroza.query.v1.mrms.get_default_r2_client", lambda: _FakeR2())

    response = await api_client.get(
        "/v1/mrms/tiles/3/2/3.png?fileKey=write-through",
        headers={"Accept": "image/webp"},
    )
    assert response.status_code == 200

    # Fire-and-forget upload — drain the in-flight set so we can
    # assert on the upload's outcome without racing it.
    from aeroza.query.v1.mrms import _INFLIGHT_WRITE_THROUGHS

    while _INFLIGHT_WRITE_THROUGHS:
        await asyncio.gather(*list(_INFLIGHT_WRITE_THROUGHS))

    assert len(uploads) == 1
    assert uploads[0]["file_key"] == "write-through"
    assert uploads[0]["z"] == 3
    assert uploads[0]["x"] == 2
    assert uploads[0]["y"] == 3
    assert uploads[0]["fmt"] == "webp"
    # Body is the rendered tile bytes — non-zero (or, if we happened
    # to land on a fully-transparent coord, ``transparent_tile_bytes``
    # is still non-zero because it's a precomputed image).
    assert uploads[0]["nbytes"] > 0


async def test_cache_hit_does_not_write_through_again(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LRU hits are the steady-state hot path; they must NOT
    re-upload to R2 — the in-memory cache already has the bytes and
    R2 has them too (from the cold-render write-through). Re-issuing
    a PUT on every hit would burn Class A ops for zero benefit."""
    from aeroza.tiles.cache import TilePngCache, set_default_cache

    set_default_cache(TilePngCache(max_bytes=1024 * 1024))

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("hit-test", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    uploads: list[str] = []

    class _CountingR2:
        async def put_tile(self, **kw: object) -> None:
            uploads.append(str(kw.get("file_key")))

    monkeypatch.setattr("aeroza.query.v1.mrms.get_default_r2_client", lambda: _CountingR2())

    # Cold render — should upload once.
    await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=hit-test")
    from aeroza.query.v1.mrms import _INFLIGHT_WRITE_THROUGHS

    while _INFLIGHT_WRITE_THROUGHS:
        await asyncio.gather(*list(_INFLIGHT_WRITE_THROUGHS))
    assert len(uploads) == 1

    # Cache hit — must NOT upload again.
    await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=hit-test")
    while _INFLIGHT_WRITE_THROUGHS:
        await asyncio.gather(*list(_INFLIGHT_WRITE_THROUGHS))
    assert len(uploads) == 1, "cache hits must not trigger redundant R2 uploads"


async def test_write_through_is_skipped_when_r2_is_unconfigured(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When AEROZA_R2_* env vars are unset (local dev, tests by
    default), ``get_default_r2_client`` returns ``None`` and the
    write-through branch is a clean no-op — the cold render path is
    otherwise identical."""
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("no-r2", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # Explicit: pretend R2 isn't configured.
    monkeypatch.setattr("aeroza.query.v1.mrms.get_default_r2_client", lambda: None)

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=no-r2")
    assert response.status_code == 200

    # Drain any in-flight tasks (defensive — there shouldn't be any).
    from aeroza.query.v1.mrms import _INFLIGHT_WRITE_THROUGHS

    while _INFLIGHT_WRITE_THROUGHS:
        await asyncio.gather(*list(_INFLIGHT_WRITE_THROUGHS))


async def test_write_through_failure_does_not_break_response(
    api_client: AsyncClient,
    integration_db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An R2 upload that 503s or 403s must not impact the client. The
    response was already serialised before the background task ran,
    and a write-through failure is logged + swallowed inside
    ``_write_through_to_r2``."""
    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("r2-blowup", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    class _ExplodingR2:
        async def put_tile(self, **_kw: object) -> None:
            raise RuntimeError("simulated R2 outage")

    monkeypatch.setattr("aeroza.query.v1.mrms.get_default_r2_client", lambda: _ExplodingR2())

    response = await api_client.get("/v1/mrms/tiles/3/2/3.png?fileKey=r2-blowup")
    assert response.status_code == 200

    # Drain the failing task so the test doesn't leak it across cases.
    from aeroza.query.v1.mrms import _INFLIGHT_WRITE_THROUGHS

    while _INFLIGHT_WRITE_THROUGHS:
        await asyncio.gather(*list(_INFLIGHT_WRITE_THROUGHS), return_exceptions=True)


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


async def test_live_tile_keeps_short_ttl_but_is_lru_cached(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """Live-mode tiles (no fileKey) keep the short TTL on the wire but
    *are* served from the LRU on the second hit. The route resolves
    "latest" to a concrete ``file_key`` before the cache lookup, so a
    live request becomes the same cache identity as a pinned request
    against the same grid — only the ``Cache-Control`` header differs
    (short max-age for live, immutable for pinned)."""
    from aeroza.tiles.cache import TilePngCache, set_default_cache

    set_default_cache(TilePngCache(max_bytes=1024 * 1024))

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    first = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert first.status_code == 200
    assert "max-age=60" in first.headers["cache-control"]
    # stale-while-revalidate lets conforming caches (iOS 17+ URLCache,
    # Cloudflare) hand back the stale tile bytes while asynchronously
    # refreshing — the radar never blanks during the 60–180s gap
    # between MRMS frame refreshes.
    assert "stale-while-revalidate=120" in first.headers["cache-control"]
    assert "immutable" not in first.headers["cache-control"]
    assert first.headers["x-aeroza-tile-cache"] == "miss"

    second = await api_client.get("/v1/mrms/tiles/3/2/3.png")
    assert second.status_code == 200
    assert "max-age=60" in second.headers["cache-control"]
    assert "stale-while-revalidate=120" in second.headers["cache-control"]
    assert second.headers["x-aeroza-tile-cache"] == "hit"
    # Bytes are byte-identical because the cached entry is the same
    # rendered tile; only the response framing differs across hits.
    assert second.content == first.content


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


async def test_off_grid_tile_short_circuits_to_transparent_bytes(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    """Tiles that sit fully outside the materialised grid's coverage
    skip the PNG/WebP encode path and reuse the precomputed
    transparent tile. The render function detects ``alpha == 0``
    everywhere and returns
    :func:`aeroza.tiles.raster.transparent_tile_bytes` verbatim — so
    the response bytes equal the bytes for an unseeded route."""
    from aeroza.tiles.raster import transparent_tile_bytes

    uri = _write_grid(tmp_path / "g.zarr")
    file = _file("k1", datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # z=3 / x=6 / y=3 is east of -45° W — fully outside the CONUS grid
    # we seeded. The render path returns alpha-zero across every
    # pixel, the fast path catches it, and we get the canned
    # transparent bytes. Explicit ``Accept: image/png`` matches the
    # ``format="png"`` we compare against (default negotiation now
    # picks WebP, which would produce different bytes).
    response = await api_client.get(
        "/v1/mrms/tiles/3/6/3.png",
        headers={"Accept": "image/png"},
    )
    assert response.status_code == 200
    expected = transparent_tile_bytes(format="png")
    assert response.content == expected


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
