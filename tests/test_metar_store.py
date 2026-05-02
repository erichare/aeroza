"""Integration tests for the METAR store: upsert + read."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from aeroza.ingest.metar import MetarObservation
from aeroza.ingest.metar_store import (
    find_latest_metar_for_station,
    list_metar_observations,
    upsert_metar_observations,
)
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(integration_db: Database) -> None:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE metar_observations"))
        await session.commit()


def _obs(
    *,
    station: str = "KIAH",
    when: datetime | None = None,
    temp_c: float | None = 32.0,
    raw: str = "TEST METAR",
    lat: float = 29.98,
    lon: float = -95.34,
) -> MetarObservation:
    return MetarObservation(
        station_id=station,
        observation_time=when or datetime(2026, 5, 2, 18, 0, tzinfo=UTC),
        latitude=lat,
        longitude=lon,
        raw_text=raw,
        temp_c=temp_c,
    )


async def test_upsert_inserts_new_rows(integration_db: Database) -> None:
    async with integration_db.sessionmaker() as session:
        result = await upsert_metar_observations(session, [_obs()])
        await session.commit()
    assert (result.inserted, result.updated) == (1, 0)


async def test_upsert_updates_when_measurement_changes(integration_db: Database) -> None:
    """Same (station_id, observation_time), different temp → row gets
    updated, not duplicated."""
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(session, [_obs(temp_c=30.0)])
        await session.commit()
    async with integration_db.sessionmaker() as session:
        result = await upsert_metar_observations(session, [_obs(temp_c=32.0)])
        await session.commit()
    assert (result.inserted, result.updated) == (0, 1)

    async with integration_db.sessionmaker() as session:
        latest = await find_latest_metar_for_station(session, station_id="KIAH")
        assert latest is not None
        assert latest.temp_c == 32.0


async def test_upsert_no_op_when_payload_unchanged(integration_db: Database) -> None:
    """Same key, same fields → predicate filters out the conflict and
    nothing returns. Useful invariant for idempotent worker re-fetches."""
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(session, [_obs()])
        await session.commit()
    async with integration_db.sessionmaker() as session:
        result = await upsert_metar_observations(session, [_obs()])
        await session.commit()
    assert (result.inserted, result.updated) == (0, 0)


async def test_upsert_empty_iterable_returns_zero(integration_db: Database) -> None:
    async with integration_db.sessionmaker() as session:
        result = await upsert_metar_observations(session, [])
        await session.commit()
    assert result.total == 0


async def test_list_filters_by_station_and_orders_newest_first(
    integration_db: Database,
) -> None:
    base = datetime(2026, 5, 2, 18, 0, tzinfo=UTC)
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(
            session,
            [
                _obs(station="KIAH", when=base),
                _obs(station="KIAH", when=base + timedelta(hours=1)),
                _obs(station="KHOU", when=base),
            ],
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        rows = await list_metar_observations(session, station_id="KIAH")
    assert [r.observation_time for r in rows] == [base + timedelta(hours=1), base]


async def test_list_filters_by_bbox(integration_db: Database) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(
            session,
            [
                _obs(station="KIAH", lat=29.98, lon=-95.34),  # inside Houston box
                _obs(station="KJFK", lat=40.64, lon=-73.78),  # outside
            ],
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        rows = await list_metar_observations(
            session,
            bbox=(-96.0, 29.0, -95.0, 30.0),
        )
    assert {r.station_id for r in rows} == {"KIAH"}


async def test_find_latest_returns_none_for_unknown_station(integration_db: Database) -> None:
    async with integration_db.sessionmaker() as session:
        latest = await find_latest_metar_for_station(session, station_id="UNKNOWN")
    assert latest is None


async def test_find_latest_returns_most_recent(integration_db: Database) -> None:
    base = datetime(2026, 5, 2, 18, 0, tzinfo=UTC)
    async with integration_db.sessionmaker() as session:
        await upsert_metar_observations(
            session,
            [
                _obs(when=base),
                _obs(when=base + timedelta(hours=2)),
                _obs(when=base + timedelta(hours=1)),
            ],
        )
        await session.commit()
    async with integration_db.sessionmaker() as session:
        latest = await find_latest_metar_for_station(session, station_id="KIAH")
        assert latest is not None
        assert latest.observation_time == base + timedelta(hours=2)
