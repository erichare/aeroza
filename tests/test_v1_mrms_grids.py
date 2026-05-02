"""End-to-end integration tests for ``GET /v1/mrms/grids[/{file_key}]``.

Seeds both ``mrms_files`` (catalog) and ``mrms_grids`` (locator), then
exercises the routes through the in-process ``api_client`` fixture.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

LIST_ROUTE: str = "/v1/mrms/grids"


def _file(
    key: str,
    *,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
    valid_at: datetime | None = None,
    size_bytes: int = 10_000,
    etag: str | None = "v1",
) -> MrmsFile:
    return MrmsFile(
        key=key,
        product=product,
        level=level,
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=size_bytes,
        etag=etag,
    )


def _locator(
    file_key: str,
    *,
    zarr_uri: str = "/var/data/mrms.zarr",
    variable: str = "reflectivity",
    dims: tuple[str, ...] = ("latitude", "longitude"),
    shape: tuple[int, ...] = (3500, 7000),
    dtype: str = "float32",
    nbytes: int = 3500 * 7000 * 4,
) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable=variable,
        dims=dims,
        shape=shape,
        dtype=dtype,
        nbytes=nbytes,
    )


async def _seed(
    integration_db: Database,
    *,
    files: tuple[MrmsFile, ...] = (),
    locators: tuple[MrmsGridLocator, ...] = (),
) -> None:
    async with integration_db.sessionmaker() as session:
        if files:
            await upsert_mrms_files(session, files)
        for loc in locators:
            await upsert_mrms_grid(session, loc)
        await session.commit()


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> object:
    yield
    async with integration_db.sessionmaker() as session:
        # CASCADE clears mrms_grids via the FK on file_key.
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


# ---------------------------------------------------------------------------
# GET /v1/mrms/grids


async def test_returns_empty_list_when_catalog_is_empty(api_client: AsyncClient) -> None:
    response = await api_client.get(LIST_ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body == {"type": "MrmsGridList", "items": []}


async def test_returns_seeded_grids_with_camelcase_wire_shape(
    api_client: AsyncClient, integration_db: Database
) -> None:
    file = _file("a", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
    await _seed(
        integration_db,
        files=(file,),
        locators=(_locator(file.key),),
    )

    response = await api_client.get(LIST_ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "MrmsGridList"
    assert len(body["items"]) == 1
    item = body["items"][0]

    assert item["fileKey"] == "a"
    assert item["product"] == "MergedReflectivityComposite"
    assert item["level"] == "00.50"
    assert item["validAt"] == "2026-05-01T12:00:00Z"
    assert item["zarrUri"] == "/var/data/mrms.zarr"
    assert item["variable"] == "reflectivity"
    assert item["dims"] == ["latitude", "longitude"]
    assert item["shape"] == [3500, 7000]
    assert item["dtype"] == "float32"
    assert item["nbytes"] == 3500 * 7000 * 4
    assert "materialisedAt" in item
    # Underscored leaks must not happen.
    assert "file_key" not in item
    assert "zarr_uri" not in item
    assert "valid_at" not in item


async def test_grids_only_returns_files_that_have_been_materialised(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """A row in ``mrms_files`` without a ``mrms_grids`` row must not appear."""
    materialised = _file("done")
    pending = _file("pending", valid_at=datetime(2026, 5, 1, 12, 1, tzinfo=UTC))
    await _seed(
        integration_db,
        files=(materialised, pending),
        locators=(_locator(materialised.key),),
    )

    keys = [item["fileKey"] for item in (await api_client.get(LIST_ROUTE)).json()["items"]]
    assert keys == ["done"]


async def test_results_are_ordered_most_recent_first(
    api_client: AsyncClient, integration_db: Database
) -> None:
    files = (
        _file("oldest", valid_at=datetime(2026, 5, 1, 11, 58, tzinfo=UTC)),
        _file("newest", valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC)),
        _file("middle", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC)),
    )
    await _seed(
        integration_db,
        files=files,
        locators=tuple(_locator(f.key) for f in files),
    )
    response = await api_client.get(LIST_ROUTE)
    keys = [item["fileKey"] for item in response.json()["items"]]
    assert keys == ["newest", "middle", "oldest"]


async def test_filters_by_product_and_level(
    api_client: AsyncClient, integration_db: Database
) -> None:
    a = _file("a", product="MergedReflectivityComposite", level="00.50")
    b = _file("b", product="PrecipRate", level="00.00")
    c = _file("c", product="PrecipRate", level="00.50")
    await _seed(
        integration_db,
        files=(a, b, c),
        locators=tuple(_locator(f.key) for f in (a, b, c)),
    )
    response = await api_client.get(LIST_ROUTE, params={"product": "PrecipRate", "level": "00.00"})
    keys = [item["fileKey"] for item in response.json()["items"]]
    assert keys == ["b"]


async def test_filters_by_since_and_until_half_open(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    files = (
        _file("before", valid_at=base - timedelta(minutes=2)),
        _file("at-since", valid_at=base),
        _file("inside", valid_at=base + timedelta(minutes=2)),
        _file("at-until", valid_at=base + timedelta(minutes=4)),
    )
    await _seed(
        integration_db,
        files=files,
        locators=tuple(_locator(f.key) for f in files),
    )
    response = await api_client.get(
        LIST_ROUTE,
        params={
            "since": base.isoformat(),
            "until": (base + timedelta(minutes=4)).isoformat(),
        },
    )
    keys = sorted(item["fileKey"] for item in response.json()["items"])
    assert keys == ["at-since", "inside"]


async def test_limit_clamps_results(api_client: AsyncClient, integration_db: Database) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    files = tuple(_file(f"k{i}", valid_at=base + timedelta(minutes=i)) for i in range(5))
    await _seed(
        integration_db,
        files=files,
        locators=tuple(_locator(f.key) for f in files),
    )
    response = await api_client.get(LIST_ROUTE, params={"limit": 2})
    assert len(response.json()["items"]) == 2


async def test_limit_above_max_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.get(LIST_ROUTE, params={"limit": 999_999})
    assert response.status_code == 422


async def test_since_after_until_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(
        LIST_ROUTE,
        params={
            "since": "2026-05-01T13:00:00+00:00",
            "until": "2026-05-01T12:00:00+00:00",
        },
    )
    assert response.status_code == 400
    assert "since must be" in response.json()["detail"]


# ---------------------------------------------------------------------------
# GET /v1/mrms/grids/{file_key}


async def test_detail_returns_404_for_unknown_key(api_client: AsyncClient) -> None:
    response = await api_client.get(f"{LIST_ROUTE}/nope")
    assert response.status_code == 404
    assert "no materialised grid" in response.json()["detail"]


async def test_detail_returns_existing_grid(
    api_client: AsyncClient, integration_db: Database
) -> None:
    file = _file("CONUS/X/20260501/MRMS_X_120000.grib2.gz")
    locator = _locator(
        file.key,
        zarr_uri="/var/data/x.zarr",
        dims=("latitude", "longitude"),
        shape=(100, 200),
    )
    await _seed(integration_db, files=(file,), locators=(locator,))

    response = await api_client.get(f"{LIST_ROUTE}/{file.key}")
    assert response.status_code == 200
    item = response.json()
    assert item["fileKey"] == file.key
    assert item["zarrUri"] == "/var/data/x.zarr"
    assert item["shape"] == [100, 200]
    assert item["dims"] == ["latitude", "longitude"]


async def test_detail_does_not_match_unmaterialised_file(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """A catalog row with no grid must 404 — not silently leak the catalog row."""
    file = _file("only-catalog")
    await _seed(integration_db, files=(file,), locators=())
    response = await api_client.get(f"{LIST_ROUTE}/only-catalog")
    assert response.status_code == 404
