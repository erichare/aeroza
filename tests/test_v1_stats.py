"""End-to-end integration tests for ``GET /v1/stats``.

Seeds alerts + MRMS files + grids in various combinations and asserts on
the snapshot's wire shape. The aggregate counts are cheap (one ``SELECT``
per counter) so the route's correctness is what we're protecting here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert, Certainty, Severity, Urgency
from aeroza.ingest.nws_alerts_store import upsert_alerts
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

ROUTE: str = "/v1/stats"


def _alert(alert_id: str, *, expires_offset: timedelta = timedelta(hours=1)) -> Alert:
    now = datetime.now(UTC)
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": "Severe Thunderstorm Warning",
            "headline": f"hl-{alert_id}",
            "severity": Severity.SEVERE,
            "urgency": Urgency.IMMEDIATE,
            "certainty": Certainty.OBSERVED,
            "sender_name": "NWS Test",
            "area_desc": "Test Area",
            "effective": now,
            "onset": now,
            "expires": now + expires_offset,
            "ends": now + expires_offset,
            "geometry": None,
        }
    )


def _file(
    key: str,
    *,
    valid_at: datetime | None = None,
) -> MrmsFile:
    return MrmsFile(
        key=key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        size_bytes=10_000,
        etag="v1",
    )


def _locator(file_key: str) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=f"/var/data/{file_key}.zarr",
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(100, 100),
        dtype="float32",
        nbytes=100 * 100 * 4,
    )


async def _seed(
    integration_db: Database,
    *,
    alerts: tuple[Alert, ...] = (),
    files: tuple[MrmsFile, ...] = (),
    locators: tuple[MrmsGridLocator, ...] = (),
) -> None:
    async with integration_db.sessionmaker() as session:
        if alerts:
            await upsert_alerts(session, alerts)
        if files:
            await upsert_mrms_files(session, files)
        for loc in locators:
            await upsert_mrms_grid(session, loc)
        await session.commit()


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE nws_alerts"))
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def test_returns_zero_counts_for_empty_database(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "Stats"
    assert "generatedAt" in body
    assert body["alerts"] == {"total": 0, "active": 0, "latestExpires": None}
    assert body["mrms"] == {
        "files": 0,
        "gridsMaterialised": 0,
        "filesPending": 0,
        "latestValidAt": None,
        "latestGridMaterialisedAt": None,
    }


async def test_separates_active_from_total_alerts(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        alerts=(
            _alert("active-1", expires_offset=timedelta(hours=1)),
            _alert("active-2", expires_offset=timedelta(hours=2)),
            _alert("expired", expires_offset=timedelta(hours=-1)),
        ),
    )
    body = (await api_client.get(ROUTE)).json()
    assert body["alerts"]["total"] == 3
    assert body["alerts"]["active"] == 2
    # Latest expiry watermark is the furthest-in-the-future row.
    assert body["alerts"]["latestExpires"] is not None


async def test_pending_files_is_files_minus_grids(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    files = (
        _file("a", valid_at=base),
        _file("b", valid_at=base + timedelta(minutes=2)),
        _file("c", valid_at=base + timedelta(minutes=4)),
    )
    # Materialise only `a` and `c`.
    await _seed(
        integration_db,
        files=files,
        locators=(_locator("a"), _locator("c")),
    )
    body = (await api_client.get(ROUTE)).json()
    assert body["mrms"]["files"] == 3
    assert body["mrms"]["gridsMaterialised"] == 2
    assert body["mrms"]["filesPending"] == 1


async def test_latest_watermarks_match_max_timestamps(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    files = (_file("a", valid_at=base), _file("b", valid_at=base + timedelta(minutes=10)))
    await _seed(
        integration_db,
        files=files,
        locators=(_locator("a"),),
    )
    body = (await api_client.get(ROUTE)).json()
    assert body["mrms"]["latestValidAt"] == "2026-05-01T12:10:00Z"
    # Materialised_at is server-set on insert; just assert it's present.
    assert body["mrms"]["latestGridMaterialisedAt"] is not None


async def test_camelcase_aliases_on_wire(api_client: AsyncClient) -> None:
    body = (await api_client.get(ROUTE)).json()
    # Underscored leaks must not happen.
    for k in (
        "generated_at",
        "latest_expires",
        "grids_materialised",
        "files_pending",
        "latest_valid_at",
        "latest_grid_materialised_at",
    ):
        assert k not in {*body.keys(), *body["alerts"].keys(), *body["mrms"].keys()}, (
            f"snake_case key {k!r} leaked through"
        )
