"""End-to-end integration tests for ``GET /v1/mrms/grids/polygon``.

Builds a small synthetic Zarr grid under ``tmp_path``, seeds the
catalog + locator pointing at it, then exercises the route through the
in-process ``api_client`` fixture. Confirms the four reducers, the
parameter validation, the latest-grid + ``at_time`` selection, and the
route-registration order vs ``/{file_key:path}``.
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

POLYGON_ROUTE: str = "/v1/mrms/grids/polygon"
PRODUCT: str = "MergedReflectivityComposite"
LEVEL: str = "00.50"


def _write_5x5_grid(target: Path) -> str:
    """5x5 grid covering lat 20..21 and lng -100..-99 at 0.25° spacing.

    Values are sequential 0..24 row-major so each test can compute the
    expected reducer output by hand.
    """
    da = xr.DataArray(
        np.arange(25, dtype=np.float32).reshape(5, 5),
        coords={
            "latitude": [20.0, 20.25, 20.5, 20.75, 21.0],
            "longitude": [-100.0, -99.75, -99.5, -99.25, -99.0],
        },
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


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
        shape=(5, 5),
        dtype="float32",
        nbytes=5 * 5 * 4,
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


# Polygon covering exactly the top-right 2x2 sub-grid.
# Cells: (lat 20.75, lng -99.25) → 18, (-99.0) → 19,
#        (lat 21.0,  lng -99.25) → 23, (-99.0) → 24.
TOP_RIGHT_2X2 = "-99.30,20.70,-98.95,20.70,-98.95,21.05,-99.30,21.05"


# ---------------------------------------------------------------------------
# Reducers


async def test_max_reducer(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get(
        POLYGON_ROUTE,
        params={"polygon": TOP_RIGHT_2X2, "reducer": "max"},
    )
    assert response.status_code == 200
    body = response.json()

    assert body["type"] == "MrmsGridPolygonSample"
    assert body["fileKey"] == "k1"
    assert body["product"] == PRODUCT
    assert body["level"] == LEVEL
    assert body["validAt"] == "2026-05-01T12:00:00Z"
    assert body["variable"] == "reflectivity"
    assert body["reducer"] == "max"
    assert body["threshold"] is None
    assert body["value"] == pytest.approx(24.0)
    assert body["cellCount"] == 4
    assert body["vertexCount"] == 4
    assert body["bboxMinLatitude"] == pytest.approx(20.75)
    assert body["bboxMaxLatitude"] == pytest.approx(21.0)
    assert body["bboxMinLongitude"] == pytest.approx(-99.25)
    assert body["bboxMaxLongitude"] == pytest.approx(-99.0)
    # No underscored leaks on the wire.
    assert "file_key" not in body
    assert "cell_count" not in body
    assert "bbox_min_latitude" not in body


async def test_mean_and_min_reducers(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    mean_resp = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "mean"}
    )
    assert mean_resp.status_code == 200
    assert mean_resp.json()["value"] == pytest.approx(21.0)  # mean(18,19,23,24)

    min_resp = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "min"}
    )
    assert min_resp.status_code == 200
    assert min_resp.json()["value"] == pytest.approx(18.0)


async def test_count_ge_reducer(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    # Whole-grid polygon → 25 cells; values >= 20 are {20..24} = 5 cells.
    whole_grid = "-100.05,19.95,-98.95,19.95,-98.95,21.05,-100.05,21.05"
    response = await api_client.get(
        POLYGON_ROUTE,
        params={
            "polygon": whole_grid,
            "reducer": "count_ge",
            "threshold": 20.0,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["reducer"] == "count_ge"
    assert body["threshold"] == 20.0
    assert body["cellCount"] == 25
    assert body["value"] == pytest.approx(5.0)


async def test_count_ge_without_threshold_is_400(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "count_ge"}
    )
    assert response.status_code == 400
    assert "threshold" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Grid selection


async def test_uses_latest_grid(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    older_uri = _write_5x5_grid(tmp_path / "older.zarr")
    # Newer grid: every value bumped by 100 so we can tell them apart.
    newer_path = tmp_path / "newer.zarr"
    xr.DataArray(
        (np.arange(25, dtype=np.float32) + 100).reshape(5, 5),
        coords={
            "latitude": [20.0, 20.25, 20.5, 20.75, 21.0],
            "longitude": [-100.0, -99.75, -99.5, -99.25, -99.0],
        },
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

    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "max"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["fileKey"] == "newer"
    assert body["value"] == pytest.approx(124.0)


async def test_at_time_picks_grid_at_or_before(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    older_uri = _write_5x5_grid(tmp_path / "older.zarr")
    newer_path = tmp_path / "newer.zarr"
    xr.DataArray(
        np.full((5, 5), 99.0, dtype=np.float32),
        coords={
            "latitude": [20.0, 20.25, 20.5, 20.75, 21.0],
            "longitude": [-100.0, -99.75, -99.5, -99.25, -99.0],
        },
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

    response = await api_client.get(
        POLYGON_ROUTE,
        params={
            "polygon": TOP_RIGHT_2X2,
            "reducer": "max",
            "at_time": (newer.valid_at - timedelta(seconds=1)).isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["fileKey"] == "older"
    assert body["value"] == pytest.approx(24.0)


# ---------------------------------------------------------------------------
# 404s


async def test_404_when_no_grid(api_client: AsyncClient, integration_db: Database) -> None:
    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "max"}
    )
    assert response.status_code == 404
    assert "no materialised grid" in response.json()["detail"]


async def test_404_when_polygon_outside_grid(
    api_client: AsyncClient, integration_db: Database, tmp_path: Path
) -> None:
    uri = _write_5x5_grid(tmp_path / "g.zarr")
    file = _file("k1", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(integration_db, (file,), (_locator(file.key, uri),))

    far_polygon = "10,50,11,50,11,51,10,51"
    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": far_polygon, "reducer": "max"}
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Validation


async def test_400_when_polygon_malformed(api_client: AsyncClient) -> None:
    response = await api_client.get(POLYGON_ROUTE, params={"polygon": "1,2,3,4", "reducer": "max"})
    # Too few vertices.
    assert response.status_code == 400


async def test_422_for_unknown_reducer(api_client: AsyncClient) -> None:
    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "median"}
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Route ordering


async def test_polygon_route_is_not_swallowed_by_grid_detail_route(
    api_client: AsyncClient,
) -> None:
    """If the route order regressed, this would 404 with 'no materialised
    grid for file_key 'polygon'' instead of running the polygon handler."""
    response = await api_client.get(
        POLYGON_ROUTE, params={"polygon": TOP_RIGHT_2X2, "reducer": "max"}
    )
    detail = response.json().get("detail", "")
    assert "file_key" not in detail
    # Either 404 "no materialised grid" (no data) or 200 — never a route mismatch.
    assert response.status_code in (200, 404)
