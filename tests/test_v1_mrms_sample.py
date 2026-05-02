"""End-to-end integration tests for ``GET /v1/mrms/grids/sample``.

Builds a small synthetic Zarr grid under ``tmp_path``, seeds the
catalog + locator pointing at it, then exercises the route through the
in-process ``api_client``. Confirms the route resolves the correct
grid (latest by valid_at, or at-or-before ``at_time``), samples
correctly, and returns clean 404s for out-of-domain / no-grid cases.
Also pins the route-registration order: the literal ``/sample`` path
must win over the ``/{file_key:path}`` matcher.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

SAMPLE_ROUTE: str = "/v1/mrms/grids/sample"
PRODUCT: str = "MergedReflectivityComposite"
LEVEL: str = "00.50"


def _write_synthetic_grid(target: Path) -> tuple[str, list[float], list[float], np.ndarray]:
    """A 3x3 grid with known values so assertions are explicit."""
    latitudes = [20.0, 20.5, 21.0]
    longitudes = [-100.0, -99.5, -99.0]
    values = np.array(
        [
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
        ],
        dtype=np.float32,
    )
    da = xr.DataArray(
        values,
        coords={"latitude": latitudes, "longitude": longitudes},
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target), latitudes, longitudes, values


def _file(key: str, *, valid_at: datetime) -> MrmsFile:
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
        shape=(3, 3),
        dtype="float32",
        nbytes=3 * 3 * 4,
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


# ---------------------------------------------------------------------------
# Happy path


async def test_samples_value_at_exact_cell(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri, _, _, _ = _write_synthetic_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 20.5, "lng": -99.5, "product": PRODUCT, "level": LEVEL},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "MrmsGridSample"
    assert body["fileKey"] == "k1"
    assert body["product"] == PRODUCT
    assert body["level"] == LEVEL
    assert body["validAt"] == "2026-05-01T12:00:00Z"
    assert body["variable"] == "reflectivity"
    assert body["value"] == pytest.approx(5.0)
    assert body["requestedLatitude"] == pytest.approx(20.5)
    assert body["requestedLongitude"] == pytest.approx(-99.5)
    assert body["matchedLatitude"] == pytest.approx(20.5)
    assert body["matchedLongitude"] == pytest.approx(-99.5)
    assert body["toleranceDeg"] == pytest.approx(0.05)
    # No underscored leaks on the wire.
    assert "file_key" not in body
    assert "valid_at" not in body
    assert "matched_latitude" not in body


async def test_samples_latest_grid_when_multiple_exist(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    older_uri, _, _, _ = _write_synthetic_grid(tmp_path / "older.zarr")
    # Newer grid: same shape but every value bumped by 100 so we can tell them apart.
    newer_path = tmp_path / "newer.zarr"
    da = xr.DataArray(
        np.array(
            [[101.0, 102.0, 103.0], [104.0, 105.0, 106.0], [107.0, 108.0, 109.0]], dtype=np.float32
        ),
        coords={"latitude": [20.0, 20.5, 21.0], "longitude": [-100.0, -99.5, -99.0]},
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    da.to_zarr(str(newer_path), mode="w")

    older = _file("older", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    newer = _file("newer", valid_at=datetime(2026, 5, 1, 12, 5, tzinfo=UTC))
    await _seed(
        integration_db,
        (older, newer),
        (_locator(older.key, older_uri), _locator(newer.key, str(newer_path))),
    )

    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 20.5, "lng": -99.5},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["fileKey"] == "newer"
    assert body["value"] == pytest.approx(105.0)


async def test_at_time_picks_grid_at_or_before(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    older_uri, _, _, _ = _write_synthetic_grid(tmp_path / "older.zarr")
    # Distinct payload for the newer grid so we can tell which one was used.
    newer_path = tmp_path / "newer.zarr"
    xr.DataArray(
        np.full((3, 3), 99.0, dtype=np.float32),
        coords={"latitude": [20.0, 20.5, 21.0], "longitude": [-100.0, -99.5, -99.0]},
        dims=("latitude", "longitude"),
        name="reflectivity",
    ).to_zarr(str(newer_path), mode="w")

    older = _file("older", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    newer = _file("newer", valid_at=datetime(2026, 5, 1, 12, 5, tzinfo=UTC))
    await _seed(
        integration_db,
        (older, newer),
        (_locator(older.key, older_uri), _locator(newer.key, str(newer_path))),
    )

    # at_time strictly before `newer` → must resolve to `older`.
    response = await api_client.get(
        SAMPLE_ROUTE,
        params={
            "lat": 20.5,
            "lng": -99.5,
            "at_time": (newer.valid_at - timedelta(seconds=1)).isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["fileKey"] == "older"
    assert body["value"] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# 404s


async def test_404_when_no_grid_exists_for_product(
    api_client: AsyncClient, integration_db: Database
) -> None:
    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 20.5, "lng": -99.5},
    )
    assert response.status_code == 404
    assert "no materialised grid" in response.json()["detail"]


async def test_404_when_at_time_predates_all_grids(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri, _, _, _ = _write_synthetic_grid(tmp_path / "g.zarr")
    file = _file("k", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get(
        SAMPLE_ROUTE,
        params={
            "lat": 20.5,
            "lng": -99.5,
            "at_time": "2026-05-01T11:00:00+00:00",  # before any grid
        },
    )
    assert response.status_code == 404


async def test_404_when_point_is_outside_domain(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri, _, _, _ = _write_synthetic_grid(tmp_path / "g.zarr")
    file = _file("k", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # Lat 50 is way outside the 20..21 grid; longitude 0 also outside.
    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 50.0, "lng": 0.0},
    )
    assert response.status_code == 404
    assert "no cell within" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Validation


async def test_lat_out_of_range_is_422(api_client: AsyncClient) -> None:
    response = await api_client.get(SAMPLE_ROUTE, params={"lat": 91.0, "lng": 0.0})
    assert response.status_code == 422


async def test_tolerance_must_be_positive(api_client: AsyncClient) -> None:
    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 0.0, "lng": 0.0, "tolerance_deg": 0.0},
    )
    assert response.status_code == 422


async def test_tolerance_capped(api_client: AsyncClient) -> None:
    response = await api_client.get(
        SAMPLE_ROUTE,
        params={"lat": 0.0, "lng": 0.0, "tolerance_deg": 5.0},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Route ordering: /sample must beat /{file_key:path}


async def test_sample_path_is_not_swallowed_by_grid_detail_route(
    api_client: AsyncClient,
) -> None:
    """If the order regressed, ``GET /v1/mrms/grids/sample`` would 404
    with 'no materialised grid for file_key 'sample'' instead of running
    the sample handler."""
    response = await api_client.get(SAMPLE_ROUTE, params={"lat": 0, "lng": 0})
    detail = response.json().get("detail", "")
    assert "file_key" not in detail
    # Either 404 "no materialised grid" (no data) or 200 — never a route mismatch.
    assert response.status_code in (200, 404)
