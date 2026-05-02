"""Integration tests for the webhook subscription store.

Exercises the CRUD helpers against a real Postgres so the schema, the
ARRAY containment query, and the CHECK constraints round-trip end to
end. The HTTP route layer is covered separately in
``test_v1_webhooks.py``.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.shared.db import Database
from aeroza.webhooks.schemas import (
    WebhookSubscriptionCreate,
    WebhookSubscriptionPatch,
)
from aeroza.webhooks.store import (
    create_subscription,
    delete_subscription,
    find_active_subscriptions_for_event,
    get_subscription,
    list_subscriptions,
    update_subscription,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions"))
        await session.commit()


def _payload(
    *,
    url: str = "https://example.com/webhook",
    events: list[str] | None = None,
    description: str | None = "test",
) -> WebhookSubscriptionCreate:
    return WebhookSubscriptionCreate(
        url=url,
        events=events or ["aeroza.alerts.nws.new"],
        description=description,
    )


# ---------------------------------------------------------------------------
# create


async def test_create_persists_row_with_generated_id_and_secret(
    db_session: AsyncSession,
) -> None:
    row = await create_subscription(db_session, _payload())
    await db_session.commit()
    assert isinstance(row.id, uuid.UUID)
    assert len(row.secret) == 64  # 32-byte hex
    assert row.status == "active"
    assert row.events == ["aeroza.alerts.nws.new"]
    assert row.created_at is not None
    assert row.updated_at is not None


async def test_create_uses_explicit_secret_when_provided(
    db_session: AsyncSession,
) -> None:
    row = await create_subscription(
        db_session,
        _payload(),
        secret="deadbeef" * 8,  # 64 hex chars
    )
    await db_session.commit()
    assert row.secret == "deadbeef" * 8


async def test_create_rejects_empty_events_via_db_constraint(
    db_session: AsyncSession,
) -> None:
    """The CHECK constraint is the load-bearing guard against empty
    arrays — pydantic also rejects ``min_length=1``, but the DB has the
    final say if anyone bypasses the API."""
    from aeroza.webhooks.models import WebhookSubscriptionRow

    db_session.add(
        WebhookSubscriptionRow(
            url="https://example.com/x",
            secret="00" * 32,
            events=[],  # bypasses the API; must still be rejected
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


# ---------------------------------------------------------------------------
# list


async def test_list_returns_newest_first(
    db_session: AsyncSession,
) -> None:
    # Commit between inserts: ``now()`` is constant within a single
    # transaction, so two inserts in the same TX share a timestamp and
    # DESC ordering becomes non-deterministic. Real callers of
    # ``create_subscription`` always commit per-request (the route does),
    # so this matches production behaviour anyway.
    a = await create_subscription(db_session, _payload(url="https://a.example.com"))
    await db_session.commit()
    b = await create_subscription(db_session, _payload(url="https://b.example.com"))
    await db_session.commit()

    rows = await list_subscriptions(db_session)
    # b was inserted second so it should sort first under DESC order on
    # ``created_at``.
    assert tuple(r.id for r in rows) == (b.id, a.id)


async def test_list_filters_by_status(db_session: AsyncSession) -> None:
    active = await create_subscription(db_session, _payload(url="https://a.example.com"))
    paused = await create_subscription(db_session, _payload(url="https://b.example.com"))
    await db_session.commit()

    await update_subscription(db_session, paused.id, WebhookSubscriptionPatch(status="paused"))
    await db_session.commit()

    active_only = await list_subscriptions(db_session, status="active")
    assert tuple(r.id for r in active_only) == (active.id,)

    paused_only = await list_subscriptions(db_session, status="paused")
    assert tuple(r.id for r in paused_only) == (paused.id,)


async def test_list_clamps_limit(db_session: AsyncSession) -> None:
    for i in range(3):
        await create_subscription(db_session, _payload(url=f"https://{i}.example.com"))
    await db_session.commit()

    rows = await list_subscriptions(db_session, limit=2)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# get


async def test_get_returns_existing_row(db_session: AsyncSession) -> None:
    created = await create_subscription(db_session, _payload())
    await db_session.commit()
    fetched = await get_subscription(db_session, created.id)
    assert fetched is not None
    assert fetched.id == created.id


async def test_get_returns_none_for_missing_id(db_session: AsyncSession) -> None:
    fetched = await get_subscription(db_session, uuid.uuid4())
    assert fetched is None


# ---------------------------------------------------------------------------
# update


async def test_update_applies_partial_patch(db_session: AsyncSession) -> None:
    row = await create_subscription(db_session, _payload(description="old"))
    await db_session.commit()
    original_updated_at = row.updated_at

    updated = await update_subscription(
        db_session,
        row.id,
        WebhookSubscriptionPatch(description="new", status="paused"),
    )
    await db_session.commit()

    assert updated is not None
    assert updated.description == "new"
    assert updated.status == "paused"
    assert updated.url == row.url  # untouched
    assert updated.updated_at >= original_updated_at


async def test_update_with_no_fields_is_noop(db_session: AsyncSession) -> None:
    row = await create_subscription(db_session, _payload())
    await db_session.commit()
    untouched = await update_subscription(db_session, row.id, WebhookSubscriptionPatch())
    assert untouched is not None
    assert untouched.id == row.id


async def test_update_returns_none_for_missing_id(db_session: AsyncSession) -> None:
    result = await update_subscription(
        db_session,
        uuid.uuid4(),
        WebhookSubscriptionPatch(description="x"),
    )
    assert result is None


async def test_update_overwrites_events_array(db_session: AsyncSession) -> None:
    row = await create_subscription(
        db_session,
        _payload(events=["aeroza.alerts.nws.new"]),
    )
    await db_session.commit()
    updated = await update_subscription(
        db_session,
        row.id,
        WebhookSubscriptionPatch(events=["aeroza.mrms.files.new", "aeroza.mrms.grids.new"]),
    )
    assert updated is not None
    assert updated.events == [
        "aeroza.mrms.files.new",
        "aeroza.mrms.grids.new",
    ]


# ---------------------------------------------------------------------------
# delete


async def test_delete_removes_row(db_session: AsyncSession) -> None:
    row = await create_subscription(db_session, _payload())
    await db_session.commit()
    deleted = await delete_subscription(db_session, row.id)
    await db_session.commit()
    assert deleted is True
    assert await get_subscription(db_session, row.id) is None


async def test_delete_returns_false_for_missing_id(db_session: AsyncSession) -> None:
    assert await delete_subscription(db_session, uuid.uuid4()) is False


# ---------------------------------------------------------------------------
# find_active_subscriptions_for_event (the dispatcher's fan-out query)


async def test_find_active_for_event_returns_matching_subscriptions(
    db_session: AsyncSession,
) -> None:
    a = await create_subscription(
        db_session,
        _payload(
            url="https://a.example.com",
            events=["aeroza.alerts.nws.new", "aeroza.mrms.files.new"],
        ),
    )
    b = await create_subscription(
        db_session,
        _payload(url="https://b.example.com", events=["aeroza.mrms.files.new"]),
    )
    await create_subscription(
        db_session,
        _payload(url="https://c.example.com", events=["aeroza.mrms.grids.new"]),
    )
    await db_session.commit()

    matches = await find_active_subscriptions_for_event(db_session, "aeroza.mrms.files.new")
    assert {row.id for row in matches} == {a.id, b.id}


async def test_find_active_excludes_paused_and_disabled(
    db_session: AsyncSession,
) -> None:
    active = await create_subscription(
        db_session,
        _payload(events=["aeroza.alerts.nws.new"]),
    )
    paused = await create_subscription(
        db_session,
        _payload(url="https://p.example.com", events=["aeroza.alerts.nws.new"]),
    )
    disabled = await create_subscription(
        db_session,
        _payload(url="https://d.example.com", events=["aeroza.alerts.nws.new"]),
    )
    await db_session.commit()

    await update_subscription(db_session, paused.id, WebhookSubscriptionPatch(status="paused"))
    await update_subscription(db_session, disabled.id, WebhookSubscriptionPatch(status="disabled"))
    await db_session.commit()

    matches = await find_active_subscriptions_for_event(db_session, "aeroza.alerts.nws.new")
    assert tuple(row.id for row in matches) == (active.id,)
