"""End-to-end integration tests for ``GET /v1/nowcasts``.

Exercises the route surface against real Postgres. Filters by
``algorithm`` / ``horizonMinutes`` are the value-add over the parallel
``/v1/mrms/grids`` route.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.nowcast.engine import PERSISTENCE_ALGORITHM
from aeroza.nowcast.store import upsert_nowcast
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

LIST_ROUTE: str = "/v1/nowcasts"


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def _seed_file(
    integration_db: Database,
    *,
    key: str,
    valid_at: datetime,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(
            session,
            (
                MrmsFile(
                    key=key,
                    product=product,
                    level=level,
                    valid_at=valid_at,
                    size_bytes=1_000,
                    etag="e",
                ),
            ),
        )
        await session.commit()


async def _seed_nowcast(
    integration_db: Database,
    *,
    source_file_key: str,
    horizon_minutes: int,
    valid_at: datetime,
    algorithm: str = PERSISTENCE_ALGORITHM,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_nowcast(
            session,
            source_file_key=source_file_key,
            product=product,
            level=level,
            algorithm=algorithm,
            horizon_minutes=horizon_minutes,
            valid_at=valid_at,
            zarr_uri=f"/tmp/{source_file_key}-{horizon_minutes}m.zarr",
            variable="reflectivity",
            dims=("latitude", "longitude"),
            shape=(3, 3),
            dtype="float32",
            nbytes=36,
        )
        await session.commit()


async def test_list_empty_when_no_nowcasts(api_client: AsyncClient) -> None:
    response = await api_client.get(LIST_ROUTE)
    assert response.status_code == 200
    assert response.json() == {"type": "NowcastList", "items": []}


async def test_list_returns_camelcase_wire_shape(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await _seed_file(integration_db, key="k1", valid_at=base)
    await _seed_nowcast(
        integration_db,
        source_file_key="k1",
        horizon_minutes=10,
        valid_at=base + timedelta(minutes=10),
    )

    response = await api_client.get(LIST_ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "NowcastList"
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["sourceFileKey"] == "k1"
    assert item["algorithm"] == "persistence"
    assert item["forecastHorizonMinutes"] == 10
    assert item["validAt"] == "2026-05-01T12:10:00Z"
    assert item["zarrUri"].startswith("/tmp/")
    # No snake_case leaks.
    assert "source_file_key" not in item
    assert "forecast_horizon_minutes" not in item


async def test_list_filters_by_algorithm_and_horizon(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await _seed_file(integration_db, key="k1", valid_at=base)
    for h in (10, 30, 60):
        await _seed_nowcast(
            integration_db,
            source_file_key="k1",
            horizon_minutes=h,
            valid_at=base + timedelta(minutes=h),
        )

    horizon10 = await api_client.get(
        LIST_ROUTE, params={"horizonMinutes": 10, "algorithm": "persistence"}
    )
    assert horizon10.status_code == 200
    items = horizon10.json()["items"]
    assert len(items) == 1
    assert items[0]["forecastHorizonMinutes"] == 10


async def test_list_rejects_inverted_window(api_client: AsyncClient) -> None:
    response = await api_client.get(
        LIST_ROUTE,
        params={
            "since": "2026-05-01T13:00:00Z",
            "until": "2026-05-01T12:00:00Z",
        },
    )
    assert response.status_code == 400
    assert "before until" in response.json()["detail"]


async def test_list_clamps_limit(api_client: AsyncClient) -> None:
    """``limit`` is bounded; the route validator surfaces 422."""
    response = await api_client.get(LIST_ROUTE, params={"limit": 100_000})
    assert response.status_code == 422
