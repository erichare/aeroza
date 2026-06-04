"""Integration test for the PostGIS device-selection query.

Marked ``integration`` — needs ``AEROZA_TEST_DATABASE_URL`` + PostGIS.
``make test`` skips it automatically.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.push.dispatch import select_devices_for_alert
from aeroza.push.models import DeviceTokenRow
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(autouse=True)
async def _clean_device_tokens(integration_db: Database) -> AsyncIterator[None]:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE device_tokens"))
        await session.commit()


async def test_select_devices_matches_only_inside_polygon(db_session: AsyncSession) -> None:
    # A square roughly covering central Oklahoma.
    await db_session.execute(
        text(
            "INSERT INTO nws_alerts (id, event, severity, geometry) VALUES "
            "('urn:test:poly', 'Tornado Warning', 'Extreme', "
            "ST_SetSRID(ST_GeomFromText("
            "'POLYGON((-98 35, -97 35, -97 36, -98 36, -98 35))'), 4326))"
        )
    )
    db_session.add_all(
        [
            DeviceTokenRow(
                token="inside",
                platform="ios",
                environment="production",
                location_lat=35.5,
                location_lng=-97.5,
            ),
            DeviceTokenRow(
                token="outside",
                platform="ios",
                environment="production",
                location_lat=40.0,
                location_lng=-100.0,
            ),
            DeviceTokenRow(token="nolocation", platform="ios", environment="production"),
        ]
    )
    await db_session.commit()

    devices = await select_devices_for_alert(db_session, "urn:test:poly")

    assert {d.token for d in devices} == {"inside"}
