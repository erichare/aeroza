"""End-to-end integration tests for ``GET /v1/mrms/files``.

Seeds the catalog via :func:`upsert_mrms_files`, then exercises the route
through the in-process ``api_client`` fixture and asserts on the wire
payload shape and filters.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

ROUTE: str = "/v1/mrms/files"


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


async def _seed(integration_db: Database, *files: MrmsFile) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(session, files)
        await session.commit()


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> object:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files"))
        await session.commit()


async def test_returns_empty_list_when_catalog_is_empty(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body == {"type": "MrmsFileList", "items": []}


async def test_returns_seeded_rows_with_camelcase_wire_shape(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _file("a", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC)),
    )
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "MrmsFileList"
    assert len(body["items"]) == 1
    item = body["items"][0]
    # Wire field names must be camelCased to match the NATS event payload.
    assert item["key"] == "a"
    assert item["product"] == "MergedReflectivityComposite"
    assert item["level"] == "00.50"
    assert item["validAt"] == "2026-05-01T12:00:00Z"
    assert item["sizeBytes"] == 10_000
    assert item["etag"] == "v1"
    # Underscored names must NOT leak through.
    assert "valid_at" not in item
    assert "size_bytes" not in item


async def test_results_are_ordered_most_recent_first(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _file("oldest", valid_at=datetime(2026, 5, 1, 11, 58, tzinfo=UTC)),
        _file("newest", valid_at=datetime(2026, 5, 1, 12, 2, tzinfo=UTC)),
        _file("middle", valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC)),
    )
    response = await api_client.get(ROUTE)
    keys = [item["key"] for item in response.json()["items"]]
    assert keys == ["newest", "middle", "oldest"]


async def test_filters_by_product(api_client: AsyncClient, integration_db: Database) -> None:
    await _seed(
        integration_db,
        _file("ref", product="MergedReflectivityComposite", level="00.50"),
        _file("rate", product="PrecipRate", level="00.00"),
    )
    response = await api_client.get(ROUTE, params={"product": "PrecipRate"})
    keys = [item["key"] for item in response.json()["items"]]
    assert keys == ["rate"]


async def test_filters_by_level(api_client: AsyncClient, integration_db: Database) -> None:
    await _seed(
        integration_db,
        _file("low", level="00.50"),
        _file("high", level="00.00"),
    )
    response = await api_client.get(ROUTE, params={"level": "00.00"})
    keys = [item["key"] for item in response.json()["items"]]
    assert keys == ["high"]


async def test_filters_by_since_and_until_half_open(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await _seed(
        integration_db,
        _file("before", valid_at=base - timedelta(minutes=2)),
        _file("at-since", valid_at=base),  # inclusive in [since, until)
        _file("inside", valid_at=base + timedelta(minutes=2)),
        _file("at-until", valid_at=base + timedelta(minutes=4)),  # exclusive
    )
    response = await api_client.get(
        ROUTE,
        params={
            "since": base.isoformat(),
            "until": (base + timedelta(minutes=4)).isoformat(),
        },
    )
    keys = sorted(item["key"] for item in response.json()["items"])
    assert keys == ["at-since", "inside"]


async def test_limit_clamps_results(api_client: AsyncClient, integration_db: Database) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await _seed(
        integration_db,
        *(_file(f"k{i}", valid_at=base + timedelta(minutes=i)) for i in range(5)),
    )
    response = await api_client.get(ROUTE, params={"limit": 2})
    assert len(response.json()["items"]) == 2


async def test_limit_above_max_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"limit": 999_999})
    assert response.status_code == 422


async def test_invalid_since_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"since": "not-a-timestamp"})
    assert response.status_code == 422


async def test_since_after_until_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(
        ROUTE,
        params={
            "since": "2026-05-01T13:00:00+00:00",
            "until": "2026-05-01T12:00:00+00:00",
        },
    )
    assert response.status_code == 400
    assert "since must be" in response.json()["detail"]


async def test_combined_product_level_window_filters(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    await _seed(
        integration_db,
        _file("matches", product="PrecipRate", level="00.00", valid_at=base),
        _file(
            "wrong-product",
            product="MergedReflectivityComposite",
            level="00.00",
            valid_at=base,
        ),
        _file(
            "wrong-level",
            product="PrecipRate",
            level="00.50",
            valid_at=base,
        ),
        _file(
            "wrong-time",
            product="PrecipRate",
            level="00.00",
            valid_at=base - timedelta(minutes=10),
        ),
    )
    response = await api_client.get(
        ROUTE,
        params={
            "product": "PrecipRate",
            "level": "00.00",
            "since": (base - timedelta(minutes=1)).isoformat(),
        },
    )
    keys = [item["key"] for item in response.json()["items"]]
    assert keys == ["matches"]
