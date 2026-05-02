"""Integration tests for the webhook dispatcher orchestrator.

Drives the orchestrator's three NATS consumers via in-memory
subscribers and a programmable httpx ``MockTransport`` that records
every POST. This covers the full slice 3 surface end-to-end against
real Postgres without needing a broker or a network endpoint.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import numpy as np
import pytest
import xarray as xr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_grids_store import upsert_mrms_grid
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert
from aeroza.shared.db import Database
from aeroza.stream.subscriber import (
    InMemoryAlertSubscriber,
    InMemoryMrmsFileSubscriber,
    InMemoryMrmsGridSubscriber,
)
from aeroza.webhooks.orchestrator import (
    RULE_FIRED_EVENT,
    run_dispatcher,
)
from aeroza.webhooks.rule_models import AlertRuleRow
from aeroza.webhooks.rule_schemas import (
    AlertRuleCreate,
    PointRuleConfig,
    PolygonRuleConfig,
    Predicate,
)
from aeroza.webhooks.rule_store import create_rule, get_rule
from aeroza.webhooks.schemas import WebhookSubscriptionCreate
from aeroza.webhooks.signing import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_signature,
)
from aeroza.webhooks.store import create_subscription, get_subscription

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions CASCADE"))
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


class CapturingTransport(httpx.MockTransport):
    """``MockTransport`` that records every request and lets each test
    inject a per-request response. Default response is 200."""

    def __init__(self, responder: Any = None) -> None:
        self.requests: list[httpx.Request] = []
        self.responder = responder
        super().__init__(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.responder is None:
            return httpx.Response(200, json={"received": True})
        return self.responder(request)


async def _make_subscription(
    session: AsyncSession,
    *,
    url: str = "https://hook.example.com/incoming",
    events: list[str] | None = None,
) -> tuple[uuid.UUID, str]:
    row = await create_subscription(
        session,
        WebhookSubscriptionCreate(
            url=url,
            events=events or ["aeroza.alerts.nws.new"],
            description=None,
        ),
        secret="deadbeef" * 8,
    )
    await session.commit()
    return row.id, row.secret


async def _make_rule(
    session: AsyncSession,
    sub_id: uuid.UUID,
    *,
    config: PointRuleConfig | PolygonRuleConfig,
) -> AlertRuleRow:
    row = await create_rule(
        session,
        AlertRuleCreate(subscription_id=sub_id, name="t", config=config),
    )
    await session.commit()
    return row


async def _seed_grid(
    session: AsyncSession,
    *,
    file_key: str,
    zarr_uri: str,
    valid_at: datetime,
    product: str = "MergedReflectivityComposite",
    level: str = "00.50",
) -> MrmsGridLocator:
    """Insert a catalog row + locator pointing at ``zarr_uri``."""
    await upsert_mrms_files(
        session,
        (
            MrmsFile(
                key=file_key,
                product=product,
                level=level,
                valid_at=valid_at,
                size_bytes=1_000,
                etag="etag",
            ),
        ),
    )
    locator = MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable="reflectivity",
        dims=("latitude", "longitude"),
        shape=(3, 3),
        dtype="float32",
        nbytes=3 * 3 * 4,
    )
    await upsert_mrms_grid(session, locator)
    await session.commit()
    return locator


def _write_grid(target: Path, *, value: float = 42.0) -> str:
    """Write a 3x3 grid centred on Houston with every cell == ``value``."""
    da = xr.DataArray(
        np.full((3, 3), value, dtype=np.float32),
        coords={
            "latitude": [29.5, 29.76, 30.0],
            "longitude": [-96.0, -95.37, -95.0],
        },
        dims=("latitude", "longitude"),
        name="reflectivity",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    da.to_zarr(str(target), mode="w")
    return str(target)


# ---------------------------------------------------------------------------
# Raw fan-out


async def test_alert_event_fans_out_to_subscribed_subscriptions(
    db_session: AsyncSession, integration_db: Database
) -> None:
    """A push to the alert subscriber → POST to every active sub
    whose ``events`` array contains the subject."""
    _a_id, a_secret = await _make_subscription(
        db_session, url="https://a.example.com", events=["aeroza.alerts.nws.new"]
    )
    b_id, _ = await _make_subscription(
        db_session, url="https://b.example.com", events=["aeroza.mrms.files.new"]
    )

    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()
    transport = CapturingTransport()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await alert_sub.wait_for_subscriber_count(1)
        await file_sub.wait_for_subscriber_count(1)
        await grid_sub.wait_for_subscriber_count(1)

        await alert_sub.push(
            Alert.model_validate({"id": "urn:1", "event": "Severe Thunderstorm Warning"})
        )
        await asyncio.sleep(0.05)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=2.0)

    # Only `a` (subscribed to alerts) received a POST.
    assert len(transport.requests) == 1
    sent = transport.requests[0]
    assert sent.url == httpx.URL("https://a.example.com")

    # Payload envelope shape.
    body = json.loads(sent.read())
    assert body["type"] == "WebhookEvent"
    assert body["event"] == "aeroza.alerts.nws.new"
    assert "deliveryId" in body
    assert body["data"]["id"] == "urn:1"

    # Signature verifies against `a`'s secret.
    verify_signature(
        payload=sent.read(),
        secret=a_secret,
        timestamp_header=sent.headers[TIMESTAMP_HEADER],
        signature_header=sent.headers[SIGNATURE_HEADER],
    )

    # `b` got nothing.
    assert b_id not in {r.url.host for r in transport.requests}


async def test_paused_subscription_is_skipped(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, _ = await _make_subscription(db_session)
    # Pause via the store before pushing the event.
    from aeroza.webhooks.schemas import WebhookSubscriptionPatch
    from aeroza.webhooks.store import update_subscription

    await update_subscription(db_session, sub_id, WebhookSubscriptionPatch(status="paused"))
    await db_session.commit()

    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()
    transport = CapturingTransport()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await alert_sub.wait_for_subscriber_count(1)
        await alert_sub.push(Alert.model_validate({"id": "urn:1", "event": "Severe"}))
        await asyncio.sleep(0.05)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=2.0)

    assert transport.requests == []


# ---------------------------------------------------------------------------
# Rule fan-out — false→true transition


async def test_grid_event_fires_rule_and_posts_to_bound_subscription(
    db_session: AsyncSession, integration_db: Database, tmp_path: Path
) -> None:
    """Predicate satisfied on first evaluation → fired_now=True → POST."""
    sub_id, secret = await _make_subscription(db_session)
    rule = await _make_rule(
        db_session,
        sub_id,
        config=PointRuleConfig(
            type="point",
            lat=29.76,
            lng=-95.37,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )

    zarr_uri = _write_grid(tmp_path / "g.zarr", value=55.0)  # all cells = 55
    locator = await _seed_grid(
        db_session,
        file_key="CONUS/MergedReflectivityComposite_00.50/20260501/k.grib2.gz",
        zarr_uri=zarr_uri,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )

    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()
    transport = CapturingTransport()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await grid_sub.wait_for_subscriber_count(1)
        await grid_sub.push(locator)
        await asyncio.sleep(0.1)  # let the orchestrator finish the tick
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=3.0)

    # Two POSTs: one raw fan-out for `aeroza.mrms.grids.new` (sub
    # isn't opted in to that event so… wait, `_make_subscription`
    # default events=["aeroza.alerts.nws.new"], so raw fan-out skips
    # this sub. Only the rule fire goes out.)
    assert len(transport.requests) == 1
    body = json.loads(transport.requests[0].read())
    assert body["event"] == RULE_FIRED_EVENT
    assert body["data"]["rule"]["id"] == str(rule.id)
    assert body["data"]["evaluation"]["value"] == 55.0
    assert body["data"]["evaluation"]["predicateSatisfied"] is True
    assert body["data"]["grid"]["fileKey"] == locator.file_key
    # Signature verifies.
    verify_signature(
        payload=transport.requests[0].read(),
        secret=secret,
        timestamp_header=transport.requests[0].headers[TIMESTAMP_HEADER],
        signature_header=transport.requests[0].headers[SIGNATURE_HEADER],
    )

    # Rule state was bumped — read with a fresh session so we don't see
    # the per-test session's cached pre-update view of the row.
    async with integration_db.sessionmaker() as fresh:
        refreshed = await get_rule(fresh, rule.id)
    assert refreshed is not None
    assert refreshed.currently_firing is True
    assert refreshed.last_value == 55.0
    assert refreshed.last_fired_at is not None


async def test_rule_does_not_re_fire_while_already_firing(
    db_session: AsyncSession, integration_db: Database, tmp_path: Path
) -> None:
    """Predicate stays true across two grids — only one POST goes out."""
    sub_id, _ = await _make_subscription(db_session)
    await _make_rule(
        db_session,
        sub_id,
        config=PointRuleConfig(
            type="point",
            lat=29.76,
            lng=-95.37,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )

    z1 = _write_grid(tmp_path / "a.zarr", value=55.0)
    z2 = _write_grid(tmp_path / "b.zarr", value=60.0)
    loc1 = await _seed_grid(
        db_session,
        file_key="k1",
        zarr_uri=z1,
        valid_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )
    loc2 = await _seed_grid(
        db_session,
        file_key="k2",
        zarr_uri=z2,
        valid_at=datetime(2026, 5, 1, 12, 5, tzinfo=UTC),
    )

    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()
    transport = CapturingTransport()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await grid_sub.wait_for_subscriber_count(1)
        await grid_sub.push(loc1)
        await asyncio.sleep(0.1)
        await grid_sub.push(loc2)
        await asyncio.sleep(0.1)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=3.0)

    # Exactly one rule-fired POST — predicate stayed true so no
    # second transition.
    rule_fired_posts = [
        r for r in transport.requests if json.loads(r.read())["event"] == RULE_FIRED_EVENT
    ]
    assert len(rule_fired_posts) == 1


async def test_rule_re_fires_after_predicate_drops_then_recovers(
    db_session: AsyncSession, integration_db: Database, tmp_path: Path
) -> None:
    """false → true → false → true: two POSTs."""
    sub_id, _ = await _make_subscription(db_session)
    await _make_rule(
        db_session,
        sub_id,
        config=PointRuleConfig(
            type="point",
            lat=29.76,
            lng=-95.37,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )

    high1 = _write_grid(tmp_path / "h1.zarr", value=55.0)
    low = _write_grid(tmp_path / "l.zarr", value=20.0)
    high2 = _write_grid(tmp_path / "h2.zarr", value=60.0)
    grids = [
        await _seed_grid(
            db_session,
            file_key=f"k{i}",
            zarr_uri=z,
            valid_at=datetime(2026, 5, 1, 12, i, tzinfo=UTC),
        )
        for i, z in enumerate((high1, low, high2))
    ]

    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()
    transport = CapturingTransport()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await grid_sub.wait_for_subscriber_count(1)
        for loc in grids:
            await grid_sub.push(loc)
            await asyncio.sleep(0.1)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=3.0)

    rule_fired_posts = [
        r for r in transport.requests if json.loads(r.read())["event"] == RULE_FIRED_EVENT
    ]
    assert len(rule_fired_posts) == 2


# ---------------------------------------------------------------------------
# Circuit breaker


async def test_consecutive_terminal_failures_disable_the_subscription(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, _ = await _make_subscription(db_session)

    transport = CapturingTransport(responder=lambda _r: httpx.Response(404, text="nope"))
    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
                auto_disable_threshold=3,
            )
        )
        await alert_sub.wait_for_subscriber_count(1)
        for i in range(3):
            await alert_sub.push(Alert.model_validate({"id": f"urn:{i}", "event": "Severe"}))
            await asyncio.sleep(0.05)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=3.0)

    async with integration_db.sessionmaker() as fresh:
        refreshed = await get_subscription(fresh, sub_id)
    assert refreshed is not None
    assert refreshed.status == "disabled"


async def test_terminal_failures_below_threshold_do_not_disable(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id, _ = await _make_subscription(db_session)

    transport = CapturingTransport(responder=lambda _r: httpx.Response(404, text="nope"))
    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()

    async with httpx.AsyncClient(transport=transport) as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
                auto_disable_threshold=5,
            )
        )
        await alert_sub.wait_for_subscriber_count(1)
        for i in range(2):
            await alert_sub.push(Alert.model_validate({"id": f"urn:{i}", "event": "Severe"}))
            await asyncio.sleep(0.05)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=3.0)

    async with integration_db.sessionmaker() as fresh:
        refreshed = await get_subscription(fresh, sub_id)
    assert refreshed is not None
    assert refreshed.status == "active"


# ---------------------------------------------------------------------------
# Lifecycle


async def test_dispatcher_exits_when_subscribers_close(
    integration_db: Database,
) -> None:
    alert_sub = InMemoryAlertSubscriber()
    file_sub = InMemoryMrmsFileSubscriber()
    grid_sub = InMemoryMrmsGridSubscriber()

    async with httpx.AsyncClient() as client:
        runner = asyncio.create_task(
            run_dispatcher(
                db=integration_db,
                http_client=client,
                alert_subscriber=alert_sub,
                file_subscriber=file_sub,
                grid_subscriber=grid_sub,
            )
        )
        await alert_sub.wait_for_subscriber_count(1)
        await file_sub.wait_for_subscriber_count(1)
        await grid_sub.wait_for_subscriber_count(1)
        await alert_sub.close()
        await file_sub.close()
        await grid_sub.close()
        await asyncio.wait_for(runner, timeout=2.0)
        assert runner.done() and runner.exception() is None
