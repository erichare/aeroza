"""End-to-end tests for ``GET /v1/metar`` and ``GET /v1/metar/{station}/latest``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.metar import MetarObservation
from aeroza.ingest.metar_store import upsert_metar_observations
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(integration_db: Database) -> None:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE metar_observations"))
        await session.commit()


async def _seed(
    integration_db: Database,
    *,
    station: str = "KIAH",
    when: datetime | None = None,
    lat: float = 29.98,
    lon: float = -95.34,
) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(
            session,
            [
                MetarObservation(
                    station_id=station,
                    observation_time=when or datetime(2026, 5, 2, 18, 0, tzinfo=UTC),
                    latitude=lat,
                    longitude=lon,
                    raw_text=f"{station} TEST",
                    temp_c=32.0,
                    wind_speed_kt=12.0,
                    flight_category="VFR",
                ),
            ],
        )
        await session.commit()


async def test_list_empty_returns_envelope(api_client: AsyncClient) -> None:
    response = await api_client.get("/v1/metar")
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "MetarObservationList"
    assert body["items"] == []


async def test_list_returns_camelcase_payload(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(integration_db)
    response = await api_client.get("/v1/metar")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    item = items[0]
    assert item["stationId"] == "KIAH"
    assert item["windSpeedKt"] == 12.0
    assert item["flightCategory"] == "VFR"
    # Snake-case must not bleed to the wire.
    for key in ("station_id", "wind_speed_kt", "flight_category"):
        assert key not in item


async def test_list_filter_by_station_is_case_insensitive(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(integration_db, station="KIAH")
    await _seed(integration_db, station="KHOU", lat=29.65, lon=-95.28)
    # Pass lowercase to confirm the route uppercases before querying.
    response = await api_client.get("/v1/metar", params={"station": "kiah"})
    items = response.json()["items"]
    assert {it["stationId"] for it in items} == {"KIAH"}


async def test_list_filter_by_bbox(api_client: AsyncClient, integration_db: Database) -> None:
    await _seed(integration_db, station="KIAH", lat=29.98, lon=-95.34)
    await _seed(integration_db, station="KJFK", lat=40.64, lon=-73.78)
    # Houston box only.
    response = await api_client.get("/v1/metar", params={"bbox": "-96.0,29.0,-95.0,30.0"})
    items = response.json()["items"]
    assert {it["stationId"] for it in items} == {"KIAH"}


async def test_list_filter_by_since(api_client: AsyncClient, integration_db: Database) -> None:
    base = datetime(2026, 5, 2, 18, 0, tzinfo=UTC)
    await _seed(integration_db, when=base)
    await _seed(integration_db, when=base + timedelta(hours=2))
    cutoff = (base + timedelta(hours=1)).isoformat()
    response = await api_client.get("/v1/metar", params={"since": cutoff})
    items = response.json()["items"]
    assert len(items) == 1
    # Only the newer one should remain.
    assert datetime.fromisoformat(items[0]["observationTime"]) > base


async def test_latest_returns_404_for_unknown_station(api_client: AsyncClient) -> None:
    response = await api_client.get("/v1/metar/KZZZ/latest")
    assert response.status_code == 404


async def test_latest_returns_most_recent(
    api_client: AsyncClient, integration_db: Database
) -> None:
    base = datetime(2026, 5, 2, 18, 0, tzinfo=UTC)
    await _seed(integration_db, when=base)
    await _seed(integration_db, when=base + timedelta(hours=2))
    response = await api_client.get("/v1/metar/KIAH/latest")
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "MetarObservation"
    assert datetime.fromisoformat(body["observationTime"]) == base + timedelta(hours=2)


async def test_validates_bbox_format(api_client: AsyncClient) -> None:
    """Malformed bbox surfaces as 400, not 500 — same as /v1/alerts."""
    response = await api_client.get("/v1/metar", params={"bbox": "not-a-bbox"})
    assert response.status_code == 400
