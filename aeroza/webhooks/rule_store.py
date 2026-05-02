"""Persistence for the alert_rules table.

CRUD plus two fan-out helpers the dispatcher (slice 3) will use:

- :func:`list_rules_for_subscription` — operator UI, "show me every
  rule under this webhook".
- :func:`find_active_rules` — dispatcher, "every rule we should
  evaluate on this tick", optionally filtered by product/level so
  the worker doesn't evaluate alerts-only rules on grid events.

The session is **not** committed by any function in this module;
callers own the transaction.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any, Final

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.webhooks.rule_models import AlertRuleRow
from aeroza.webhooks.rule_schemas import (
    AlertRuleCreate,
    AlertRulePatch,
    config_to_jsonb,
)

log = structlog.get_logger(__name__)

DEFAULT_LIST_LIMIT: Final[int] = 100
MAX_LIST_LIMIT: Final[int] = 500


async def create_rule(
    session: AsyncSession,
    payload: AlertRuleCreate,
) -> AlertRuleRow:
    row = AlertRuleRow(
        subscription_id=payload.subscription_id,
        name=payload.name,
        description=payload.description,
        rule_type=payload.config.type,
        config=config_to_jsonb(payload.config),
    )
    session.add(row)
    await session.flush()
    log.info(
        "alert_rules.store.create",
        id=str(row.id),
        subscription_id=str(row.subscription_id),
        rule_type=row.rule_type,
    )
    return row


async def get_rule(session: AsyncSession, rule_id: uuid.UUID) -> AlertRuleRow | None:
    result = await session.execute(select(AlertRuleRow).where(AlertRuleRow.id == rule_id))
    return result.scalar_one_or_none()


async def list_rules(
    session: AsyncSession,
    *,
    status: str | None = None,
    subscription_id: uuid.UUID | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
) -> tuple[AlertRuleRow, ...]:
    """Return rules ordered by ``created_at`` descending."""
    bounded_limit = min(max(limit, 1), MAX_LIST_LIMIT)
    stmt = select(AlertRuleRow).order_by(AlertRuleRow.created_at.desc()).limit(bounded_limit)
    if status is not None:
        stmt = stmt.where(AlertRuleRow.status == status)
    if subscription_id is not None:
        stmt = stmt.where(AlertRuleRow.subscription_id == subscription_id)
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


async def list_rules_for_subscription(
    session: AsyncSession, subscription_id: uuid.UUID
) -> Sequence[AlertRuleRow]:
    """Convenience: every rule under one subscription, newest first."""
    result = await session.execute(
        select(AlertRuleRow)
        .where(AlertRuleRow.subscription_id == subscription_id)
        .order_by(AlertRuleRow.created_at.desc())
    )
    return tuple(result.scalars().all())


async def update_rule(
    session: AsyncSession,
    rule_id: uuid.UUID,
    patch: AlertRulePatch,
) -> AlertRuleRow | None:
    """Apply a partial update. Returns the refreshed row or ``None``."""
    values: dict[str, object] = {}
    if patch.name is not None:
        values["name"] = patch.name
    if patch.description is not None:
        values["description"] = patch.description
    if patch.status is not None:
        values["status"] = patch.status
    if patch.config is not None:
        values["rule_type"] = patch.config.type
        values["config"] = config_to_jsonb(patch.config)

    if not values:
        return await get_rule(session, rule_id)

    values["updated_at"] = func.now()
    result = await session.execute(
        update(AlertRuleRow)
        .where(AlertRuleRow.id == rule_id)
        .values(**values)
        .returning(AlertRuleRow)
    )
    row = result.scalar_one_or_none()
    if row is not None:
        await session.refresh(row)
    log.info(
        "alert_rules.store.update",
        id=str(rule_id),
        fields=list(values.keys()),
        found=row is not None,
    )
    return row


async def update_rule_evaluation(
    session: AsyncSession,
    rule_id: uuid.UUID,
    *,
    last_value: float | None,
    currently_firing: bool,
    fired_now: bool,
) -> None:
    """Persist the dispatcher's per-tick state. Slice 3 uses this.

    ``fired_now`` toggles ``last_fired_at = now()`` when the rule
    transitioned from not-firing to firing. ``currently_firing``
    reflects the new state regardless. Kept on the store so slice 3's
    dispatcher doesn't grow its own SQL.
    """
    values: dict[str, Any] = {
        "last_value": last_value,
        "currently_firing": currently_firing,
        "last_evaluated_at": func.now(),
    }
    if fired_now:
        values["last_fired_at"] = func.now()

    await session.execute(update(AlertRuleRow).where(AlertRuleRow.id == rule_id).values(**values))


async def delete_rule(session: AsyncSession, rule_id: uuid.UUID) -> bool:
    result = await session.execute(
        delete(AlertRuleRow).where(AlertRuleRow.id == rule_id).returning(AlertRuleRow.id)
    )
    deleted = result.scalar_one_or_none() is not None
    log.info("alert_rules.store.delete", id=str(rule_id), deleted=deleted)
    return deleted


async def find_active_rules(
    session: AsyncSession,
    *,
    product: str | None = None,
    level: str | None = None,
) -> Sequence[AlertRuleRow]:
    """Return every active rule, newest first.

    The dispatcher (slice 3) calls this on each grid event.
    ``product``/``level`` filters narrow the set when the dispatcher
    knows which grid it's about to evaluate against — JSONB ``->>``
    keys are stable on the camelCase keys we persist.
    """
    stmt = (
        select(AlertRuleRow)
        .where(AlertRuleRow.status == "active")
        .order_by(AlertRuleRow.created_at.desc())
    )
    if product is not None:
        stmt = stmt.where(AlertRuleRow.config["product"].astext == product)
    if level is not None:
        stmt = stmt.where(AlertRuleRow.config["level"].astext == level)
    result = await session.execute(stmt)
    return tuple(result.scalars().all())


__all__ = [
    "DEFAULT_LIST_LIMIT",
    "MAX_LIST_LIMIT",
    "create_rule",
    "delete_rule",
    "find_active_rules",
    "get_rule",
    "list_rules",
    "list_rules_for_subscription",
    "update_rule",
    "update_rule_evaluation",
]
