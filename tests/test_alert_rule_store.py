"""Integration tests for the alert-rule store.

Exercises CRUD + the dispatcher fan-out helpers
(``find_active_rules``, ``list_rules_for_subscription``,
``update_rule_evaluation``) against real Postgres so the JSONB config
column, the FK to ``webhook_subscriptions``, and the JSONB ``->>``
filters all round-trip end-to-end.
"""

from __future__ import annotations

import uuid
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.shared.db import Database
from aeroza.webhooks.rule_models import AlertRuleRow
from aeroza.webhooks.rule_schemas import (
    AlertRuleCreate,
    AlertRulePatch,
    PointRuleConfig,
    PolygonRuleConfig,
    Predicate,
)
from aeroza.webhooks.rule_store import (
    create_rule,
    delete_rule,
    find_active_rules,
    get_rule,
    list_rules,
    list_rules_for_subscription,
    update_rule,
    update_rule_evaluation,
)
from aeroza.webhooks.schemas import WebhookSubscriptionCreate
from aeroza.webhooks.store import create_subscription

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        # CASCADE on the FK takes alert_rules with it.
        await session.execute(text("TRUNCATE TABLE webhook_subscriptions CASCADE"))
        await session.commit()


async def _make_subscription(session: AsyncSession, *, url: str = "https://x.example.com") -> UUID:
    row = await create_subscription(
        session,
        WebhookSubscriptionCreate(
            url=url,
            events=["aeroza.mrms.grids.new"],
            description=None,
        ),
    )
    await session.commit()
    return row.id


def _point_create(sub_id: UUID, *, name: str = "Houston ≥ 40") -> AlertRuleCreate:
    return AlertRuleCreate(
        subscription_id=sub_id,
        name=name,
        config=PointRuleConfig(
            type="point",
            lat=29.76,
            lng=-95.37,
            product="MergedReflectivityComposite",
            level="00.50",
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )


def _polygon_create(sub_id: UUID, *, name: str = "Region max ≥ 40") -> AlertRuleCreate:
    return AlertRuleCreate(
        subscription_id=sub_id,
        name=name,
        config=PolygonRuleConfig(
            type="polygon",
            polygon="-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
            reducer="max",
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )


# ---------------------------------------------------------------------------
# create


async def test_create_persists_point_rule_with_jsonb_config(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    row = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()

    assert isinstance(row.id, uuid.UUID)
    assert row.subscription_id == sub_id
    assert row.rule_type == "point"
    assert row.status == "active"
    assert row.currently_firing is False
    # Discriminator stripped from JSONB; column is the source of truth.
    assert "type" not in row.config
    assert row.config["lat"] == 29.76
    assert row.config["predicate"]["op"] == ">="


async def test_create_polygon_rule_persists_reducer_and_polygon(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    row = await create_rule(db_session, _polygon_create(sub_id))
    await db_session.commit()

    assert row.rule_type == "polygon"
    assert row.config["reducer"] == "max"
    assert "29.5" in row.config["polygon"]


async def test_create_rejects_invalid_rule_type_via_db_check(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    db_session.add(
        AlertRuleRow(
            subscription_id=sub_id,
            name="bad",
            rule_type="weird",
            config={"any": "thing"},
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.flush()


async def test_create_cascades_on_subscription_delete(
    db_session: AsyncSession, integration_db: Database
) -> None:
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()

    # Delete the subscription via raw SQL (the store doesn't expose
    # delete-with-cascade since FK does the work). The rule should
    # disappear with it.
    async with integration_db.sessionmaker() as session:
        await session.execute(
            text("DELETE FROM webhook_subscriptions WHERE id = :id"),
            {"id": sub_id},
        )
        await session.commit()

    assert await get_rule(db_session, rule.id) is None


# ---------------------------------------------------------------------------
# list / get / update / delete


async def test_list_filters_by_subscription_and_status(
    db_session: AsyncSession,
) -> None:
    sub_a = await _make_subscription(db_session, url="https://a.example.com")
    sub_b = await _make_subscription(db_session, url="https://b.example.com")

    await create_rule(db_session, _point_create(sub_a, name="a-1"))
    rule_a2 = await create_rule(db_session, _point_create(sub_a, name="a-2"))
    await create_rule(db_session, _polygon_create(sub_b, name="b-1"))
    await db_session.commit()

    await update_rule(db_session, rule_a2.id, AlertRulePatch(status="paused"))
    await db_session.commit()

    only_a = await list_rules(db_session, subscription_id=sub_a)
    assert {r.subscription_id for r in only_a} == {sub_a}
    assert {r.name for r in only_a} == {"a-1", "a-2"}

    paused = await list_rules(db_session, status="paused")
    assert {r.id for r in paused} == {rule_a2.id}


async def test_list_rules_for_subscription_returns_only_matching_rows(
    db_session: AsyncSession,
) -> None:
    sub_a = await _make_subscription(db_session)
    sub_b = await _make_subscription(db_session, url="https://b.example.com")
    await create_rule(db_session, _point_create(sub_a))
    await create_rule(db_session, _point_create(sub_b))
    await db_session.commit()

    rows = await list_rules_for_subscription(db_session, sub_a)
    assert all(r.subscription_id == sub_a for r in rows)


async def test_update_replaces_config_wholesale(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()

    updated = await update_rule(
        db_session,
        rule.id,
        AlertRulePatch(
            config=PolygonRuleConfig(
                type="polygon",
                polygon="-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
                reducer="mean",
                predicate=Predicate(op="<", threshold=20.0),
            )
        ),
    )
    assert updated is not None
    assert updated.rule_type == "polygon"
    assert updated.config["reducer"] == "mean"


async def test_update_with_no_fields_returns_existing_row(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()
    untouched = await update_rule(db_session, rule.id, AlertRulePatch())
    assert untouched is not None
    assert untouched.id == rule.id


async def test_delete_removes_row_and_returns_false_for_missing(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()
    assert await delete_rule(db_session, rule.id) is True
    await db_session.commit()
    assert await delete_rule(db_session, rule.id) is False


# ---------------------------------------------------------------------------
# find_active_rules / update_rule_evaluation — dispatcher surfaces


async def test_find_active_rules_filters_by_product_and_level(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)

    # Default product/level rule.
    a = await create_rule(db_session, _point_create(sub_id, name="default-product"))
    # Different product.
    b_create = AlertRuleCreate(
        subscription_id=sub_id,
        name="other-product",
        config=PointRuleConfig(
            type="point",
            lat=0,
            lng=0,
            product="PrecipRate",
            level="00.00",
            predicate=Predicate(op=">", threshold=0.0),
        ),
    )
    b = await create_rule(db_session, b_create)
    await db_session.commit()

    matches = await find_active_rules(
        db_session, product="MergedReflectivityComposite", level="00.50"
    )
    assert {r.id for r in matches} == {a.id}

    matches_b = await find_active_rules(db_session, product="PrecipRate", level="00.00")
    assert {r.id for r in matches_b} == {b.id}


async def test_find_active_rules_excludes_paused_and_disabled(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    active = await create_rule(db_session, _point_create(sub_id, name="active"))
    paused = await create_rule(db_session, _point_create(sub_id, name="paused"))
    disabled = await create_rule(db_session, _point_create(sub_id, name="disabled"))
    await db_session.commit()

    await update_rule(db_session, paused.id, AlertRulePatch(status="paused"))
    await update_rule(db_session, disabled.id, AlertRulePatch(status="disabled"))
    await db_session.commit()

    matches = await find_active_rules(db_session)
    assert {r.id for r in matches} == {active.id}


async def test_update_rule_evaluation_sets_state(
    db_session: AsyncSession,
) -> None:
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()

    await update_rule_evaluation(
        db_session,
        rule.id,
        last_value=42.5,
        currently_firing=True,
        fired_now=True,
    )
    await db_session.commit()
    refreshed = await get_rule(db_session, rule.id)
    assert refreshed is not None
    assert refreshed.last_value == 42.5
    assert refreshed.currently_firing is True
    assert refreshed.last_evaluated_at is not None
    assert refreshed.last_fired_at is not None


async def test_update_rule_evaluation_only_bumps_fired_at_when_fired_now(
    db_session: AsyncSession,
) -> None:
    """A rule that's already firing on a tick re-evaluates without
    bumping ``last_fired_at`` — the dispatcher distinguishes 'still
    firing' from 'newly fired'."""
    sub_id = await _make_subscription(db_session)
    rule = await create_rule(db_session, _point_create(sub_id))
    await db_session.commit()

    await update_rule_evaluation(
        db_session, rule.id, last_value=42.0, currently_firing=True, fired_now=True
    )
    await db_session.commit()
    first = await get_rule(db_session, rule.id)
    assert first is not None
    first_fired_at = first.last_fired_at

    await update_rule_evaluation(
        db_session, rule.id, last_value=43.0, currently_firing=True, fired_now=False
    )
    await db_session.commit()
    second = await get_rule(db_session, rule.id)
    assert second is not None
    assert second.last_fired_at == first_fired_at  # unchanged
    assert second.last_value == 43.0
