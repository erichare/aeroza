"""Integration tests for the poll_nws_alerts_once orchestrator.

Uses a stub async fetcher (no HTTP) to deliver controlled alert batches
across multiple ticks, the real Postgres+PostGIS via the integration_db
fixture, and an InMemoryAlertPublisher to verify which alerts produced
"new" events.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from sqlalchemy import text

from aeroza.ingest.nws_alerts import Alert, Certainty, Severity, Urgency
from aeroza.ingest.poll import poll_nws_alerts_once
from aeroza.shared.db import Database
from aeroza.stream.publisher import AlertPublisher, InMemoryAlertPublisher

pytestmark = pytest.mark.integration


def _alert(alert_id: str, *, headline: str = "initial") -> Alert:
    now = datetime.now(UTC)
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": "Severe Thunderstorm Warning",
            "headline": headline,
            "severity": Severity.SEVERE,
            "urgency": Urgency.IMMEDIATE,
            "certainty": Certainty.OBSERVED,
            "sender_name": "NWS Test",
            "area_desc": "Test Area",
            "effective": now,
            "onset": now,
            "expires": now + timedelta(hours=1),
            "ends": now + timedelta(hours=1),
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        }
    )


def _stub_fetcher(*alerts: Alert) -> Any:
    async def _fetch() -> tuple[Alert, ...]:
        return tuple(alerts)

    return _fetch


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE nws_alerts"))
        await session.commit()


async def test_publishes_new_alerts_on_first_tick(integration_db: Database) -> None:
    publisher = InMemoryAlertPublisher()
    fetcher = _stub_fetcher(_alert("a"), _alert("b"))

    result = await poll_nws_alerts_once(db=integration_db, publisher=publisher, fetcher=fetcher)

    assert result.inserted_ids == ("a", "b")
    assert result.updated_ids == ()
    assert publisher.published_ids == ("a", "b")


async def test_does_not_publish_for_no_op_updates(integration_db: Database) -> None:
    publisher = InMemoryAlertPublisher()
    same = _alert("a", headline="same")

    first = await poll_nws_alerts_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher(same)
    )
    assert first.inserted_ids == ("a",)
    assert publisher.published_ids == ("a",)

    publisher.clear()
    second = await poll_nws_alerts_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher(same)
    )
    assert second.inserted_ids == ()
    assert second.updated_ids == ()
    assert publisher.published_ids == ()


async def test_does_not_publish_for_real_updates(integration_db: Database) -> None:
    publisher = InMemoryAlertPublisher()

    first = await poll_nws_alerts_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_alert("a", headline="v1")),
    )
    assert first.inserted_ids == ("a",)
    publisher.clear()

    second = await poll_nws_alerts_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_alert("a", headline="v2")),
    )
    assert second.inserted_ids == ()
    assert second.updated_ids == ("a",)
    # Updates are NOT events — only newly-observed ids are.
    assert publisher.published_ids == ()


async def test_mixed_batch_publishes_only_new(integration_db: Database) -> None:
    publisher = InMemoryAlertPublisher()
    await poll_nws_alerts_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_alert("a", headline="v1")),
    )
    publisher.clear()

    result = await poll_nws_alerts_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(
            _alert("a", headline="v2"),  # update
            _alert("b"),  # insert
        ),
    )

    assert set(result.inserted_ids) == {"b"}
    assert set(result.updated_ids) == {"a"}
    assert publisher.published_ids == ("b",)


async def test_publisher_failures_do_not_break_persistence(integration_db: Database) -> None:
    """A flaky publisher must NOT roll back the upsert. The data is durably
    persisted; a future tick or a replay job will catch up on missed events."""

    class FlakyPublisher:
        async def publish_new_alert(self, alert: Alert) -> None:
            raise RuntimeError(f"transport down for {alert.id}")

    publisher: AlertPublisher = FlakyPublisher()
    structlog.contextvars.clear_contextvars()

    result = await poll_nws_alerts_once(
        db=integration_db,
        publisher=publisher,
        fetcher=_stub_fetcher(_alert("a"), _alert("b")),
    )

    # The upsert succeeded even though publish failed.
    assert result.inserted == 2
    async with integration_db.sessionmaker() as session:
        count = (await session.execute(text("SELECT COUNT(*) FROM nws_alerts"))).scalar_one()
        assert count == 2


async def test_empty_fetch_is_safe(integration_db: Database) -> None:
    publisher = InMemoryAlertPublisher()
    result = await poll_nws_alerts_once(
        db=integration_db, publisher=publisher, fetcher=_stub_fetcher()
    )
    assert result.inserted_ids == ()
    assert result.updated_ids == ()
    assert publisher.published_ids == ()
