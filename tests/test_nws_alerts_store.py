"""Integration tests for the NWS alerts persistence layer.

Exercises the real Postgres+PostGIS instance: round-trips an Alert, verifies
the upsert distinguishes inserts from updates, and confirms geometry survives
the trip via ``ST_AsGeoJSON``. Skipped automatically when
``AEROZA_TEST_DATABASE_URL`` is unset or the database is unreachable.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.nws_alerts import Alert, Certainty, Severity, Urgency
from aeroza.ingest.nws_alerts_models import NwsAlertRow
from aeroza.ingest.nws_alerts_store import upsert_alerts

pytestmark = pytest.mark.integration


def _polygon_geom() -> dict[str, Any]:
    return {
        "type": "Polygon",
        "coordinates": [
            [[-95.7, 29.5], [-95.7, 30.0], [-95.0, 30.0], [-95.0, 29.5], [-95.7, 29.5]]
        ],
    }


def _alert(alert_id: str = "urn:test:alert:001", **overrides: Any) -> Alert:
    base: dict[str, Any] = {
        "id": alert_id,
        "event": "Severe Thunderstorm Warning",
        "headline": "Severe Thunderstorm Warning until 8:30 PM CDT",
        "description": "At 745 PM CDT, a severe thunderstorm was located...",
        "instruction": "Move to an interior room.",
        "severity": Severity.SEVERE,
        "urgency": Urgency.IMMEDIATE,
        "certainty": Certainty.OBSERVED,
        "sender_name": "NWS Houston/Galveston TX",
        "area_desc": "Harris County, TX",
        "effective": datetime(2026, 5, 1, 0, 45, tzinfo=UTC),
        "onset": datetime(2026, 5, 1, 0, 45, tzinfo=UTC),
        "expires": datetime(2026, 5, 1, 1, 30, tzinfo=UTC),
        "ends": datetime(2026, 5, 1, 1, 30, tzinfo=UTC),
        "geometry": _polygon_geom(),
    }
    base.update(overrides)
    return Alert.model_validate(base)


async def test_inserts_new_alert(db_session: AsyncSession) -> None:
    result = await upsert_alerts(db_session, [_alert()])
    await db_session.commit()

    assert result.inserted_ids == ("urn:test:alert:001",)
    assert result.updated_ids == ()
    assert result.inserted == 1
    assert result.updated == 0

    rows = (await db_session.execute(select(NwsAlertRow))).scalars().all()
    assert len(rows) == 1
    row = rows[0]
    assert row.event == "Severe Thunderstorm Warning"
    assert row.severity == "Severe"
    assert row.urgency == "Immediate"
    assert row.area_desc == "Harris County, TX"
    assert row.expires == datetime(2026, 5, 1, 1, 30, tzinfo=UTC)
    assert row.geometry is not None


async def test_empty_input_is_noop(db_session: AsyncSession) -> None:
    result = await upsert_alerts(db_session, [])
    assert result.inserted_ids == ()
    assert result.updated_ids == ()
    assert result.total == 0


async def test_updates_existing_alert_on_conflict(db_session: AsyncSession) -> None:
    first = await upsert_alerts(db_session, [_alert(headline="initial")])
    await db_session.commit()
    assert first.inserted_ids == ("urn:test:alert:001",)

    second = await upsert_alerts(db_session, [_alert(headline="revised")])
    await db_session.commit()
    assert second.inserted_ids == ()
    assert second.updated_ids == ("urn:test:alert:001",)

    row = (await db_session.execute(select(NwsAlertRow))).scalar_one()
    assert row.headline == "revised"
    assert row.inserted_at <= row.updated_at


async def test_no_op_update_skipped_via_where_clause(db_session: AsyncSession) -> None:
    alert = _alert()
    await upsert_alerts(db_session, [alert])
    await db_session.commit()

    initial = (await db_session.execute(select(NwsAlertRow))).scalar_one()
    initial_updated_at = initial.updated_at

    second = await upsert_alerts(db_session, [alert])
    await db_session.commit()

    assert second.inserted_ids == ()
    assert second.updated_ids == ()  # unchanged row was filtered out by WHERE

    refreshed = (
        await db_session.execute(select(NwsAlertRow).where(NwsAlertRow.id == alert.id))
    ).scalar_one()
    assert refreshed.updated_at == initial_updated_at


async def test_mixed_insert_and_update_in_one_call(db_session: AsyncSession) -> None:
    await upsert_alerts(db_session, [_alert(alert_id="a", headline="v1")])
    await db_session.commit()

    result = await upsert_alerts(
        db_session,
        [
            _alert(alert_id="a", headline="v2"),  # update
            _alert(alert_id="b", headline="new"),  # insert
        ],
    )
    await db_session.commit()

    assert result.inserted_ids == ("b",)
    assert result.updated_ids == ("a",)
    assert result.total == 2


async def test_geometry_is_persisted_as_postgis_polygon(db_session: AsyncSession) -> None:
    await upsert_alerts(db_session, [_alert()])
    await db_session.commit()

    geojson = (
        await db_session.execute(text("SELECT ST_AsGeoJSON(geometry) FROM nws_alerts"))
    ).scalar_one()
    parsed = json.loads(geojson)
    assert parsed["type"] == "Polygon"
    assert parsed["coordinates"][0][0] == [-95.7, 29.5]


async def test_alert_without_geometry_persists_with_null(db_session: AsyncSession) -> None:
    result = await upsert_alerts(db_session, [_alert(alert_id="no-geom", geometry=None)])
    await db_session.commit()

    assert result.inserted_ids == ("no-geom",)
    row = (await db_session.execute(select(NwsAlertRow))).scalar_one()
    assert row.geometry is None
