"""Webhook dispatcher — the long-running worker.

Consumes the three NATS subjects (alerts, MRMS files, MRMS grids),
fans each event out to every active subscription that asked for it
(raw fan-out), and — for grid events — also evaluates active alert
rules and POSTs to bound subscriptions on a false→true predicate
transition (rule fan-out).

The worker is built around three independent consumer tasks (one per
subject) running under a single :class:`asyncio.TaskGroup`. Per-event
work is sequential within a consumer; the deliberate simplicity is
why this whole module fits in one file. Horizontal scaling, when we
need it, comes from running multiple worker processes with NATS
queue groups — the in-process design changes nothing about that.

Design notes
------------

- **Sessions are short-lived.** The store + delivery primitives use
  per-call sessions so a long-running consumer doesn't pin a
  connection while waiting on HTTP. The HTTP client is shared.
- **Circuit breaker is simple.** After
  :data:`AUTO_DISABLE_CONSECUTIVE_FAILURES` consecutive
  ``status='failed'`` rows for one subscription, mark it
  ``disabled``. The dispatcher rechecks per-delivery; the operator
  re-enables via the existing ``PATCH`` route.
- **Rule fires bypass the subscription's ``events`` array.** A rule
  is bound to a subscription explicitly; the operator opted in by
  creating the rule. Slice 1's ``events`` array gates raw fan-out
  only.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Final

import httpx
import structlog
from sqlalchemy import select

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.query.mrms_grids import find_mrms_grid_by_key
from aeroza.query.mrms_sample import (
    GridSample,
    PolygonSample,
    sample_grid_at_point,
    sample_grid_in_polygon,
)
from aeroza.query.parsers import parse_polygon
from aeroza.shared.db import Database
from aeroza.stream.nats import (
    MRMS_NEW_FILE_SUBJECT,
    MRMS_NEW_GRID_SUBJECT,
    NOWCAST_NEW_GRID_SUBJECT,
    NWS_NEW_ALERT_SUBJECT,
)
from aeroza.stream.subscriber import (
    AlertSubscriber,
    MrmsFileSubscriber,
    MrmsGridSubscriber,
    NowcastGridSubscriber,
)
from aeroza.webhooks.delivery import (
    DeliveryOutcome,
    DeliveryRequest,
    deliver_payload,
)
from aeroza.webhooks.delivery_models import WebhookDeliveryRow
from aeroza.webhooks.rule_evaluator import (
    PointSampler,
    PolygonSampler,
    RuleEvaluation,
    evaluate_rule,
)
from aeroza.webhooks.rule_models import AlertRuleRow
from aeroza.webhooks.rule_schemas import alert_rule_from_row
from aeroza.webhooks.rule_store import find_active_rules, update_rule_evaluation
from aeroza.webhooks.schemas import WebhookSubscriptionPatch
from aeroza.webhooks.store import (
    find_active_subscriptions_for_event,
    get_subscription,
    update_subscription,
)

log = structlog.get_logger(__name__)

RULE_FIRED_EVENT: Final[str] = "aeroza.alert_rules.fired"
AUTO_DISABLE_CONSECUTIVE_FAILURES: Final[int] = 5


async def run_dispatcher(
    *,
    db: Database,
    http_client: httpx.AsyncClient,
    alert_subscriber: AlertSubscriber,
    file_subscriber: MrmsFileSubscriber,
    grid_subscriber: MrmsGridSubscriber,
    nowcast_subscriber: NowcastGridSubscriber,
    auto_disable_threshold: int = AUTO_DISABLE_CONSECUTIVE_FAILURES,
) -> None:
    """Run the dispatcher until every consumer's stream ends.

    Returns when every consumer has completed (e.g. the in-memory
    subscribers' ``close()`` was called, or the broker disconnected).
    Per-event exceptions inside a consumer are logged but never
    propagate out — one bad delivery must not tear the worker down.
    """
    log.info("webhooks.dispatcher.start")
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                _consume_alerts(db, http_client, alert_subscriber, auto_disable_threshold),
                name="webhooks.dispatcher.alerts",
            )
            tg.create_task(
                _consume_files(db, http_client, file_subscriber, auto_disable_threshold),
                name="webhooks.dispatcher.files",
            )
            tg.create_task(
                _consume_grids(db, http_client, grid_subscriber, auto_disable_threshold),
                name="webhooks.dispatcher.grids",
            )
            tg.create_task(
                _consume_nowcasts(db, http_client, nowcast_subscriber, auto_disable_threshold),
                name="webhooks.dispatcher.nowcasts",
            )
    finally:
        log.info("webhooks.dispatcher.stop")


# ---------------------------------------------------------------------------
# Consumers


async def _consume_alerts(
    db: Database,
    http_client: httpx.AsyncClient,
    subscriber: AlertSubscriber,
    auto_disable_threshold: int,
) -> None:
    async for alert in subscriber.subscribe_new_alerts():
        try:
            await dispatch_event(
                db,
                http_client,
                event_type=NWS_NEW_ALERT_SUBJECT,
                event_data=alert.model_dump(by_alias=True, mode="json"),
                auto_disable_threshold=auto_disable_threshold,
            )
        except Exception as exc:
            log.exception(
                "webhooks.dispatcher.alert_event_failed",
                alert_id=getattr(alert, "id", None),
                error=str(exc),
            )


async def _consume_files(
    db: Database,
    http_client: httpx.AsyncClient,
    subscriber: MrmsFileSubscriber,
    auto_disable_threshold: int,
) -> None:
    async for file in subscriber.subscribe_new_files():
        try:
            await dispatch_event(
                db,
                http_client,
                event_type=MRMS_NEW_FILE_SUBJECT,
                event_data=_file_to_dict(file),
                auto_disable_threshold=auto_disable_threshold,
            )
        except Exception as exc:
            log.exception(
                "webhooks.dispatcher.file_event_failed",
                key=file.key,
                error=str(exc),
            )


async def _consume_grids(
    db: Database,
    http_client: httpx.AsyncClient,
    subscriber: MrmsGridSubscriber,
    auto_disable_threshold: int,
) -> None:
    async for locator in subscriber.subscribe_new_grids():
        try:
            await dispatch_event(
                db,
                http_client,
                event_type=MRMS_NEW_GRID_SUBJECT,
                event_data=_locator_to_dict(locator),
                auto_disable_threshold=auto_disable_threshold,
            )
        except Exception as exc:
            log.exception(
                "webhooks.dispatcher.grid_event_failed",
                key=locator.file_key,
                error=str(exc),
            )

        try:
            await evaluate_rules_for_grid(
                db,
                http_client,
                locator,
                auto_disable_threshold=auto_disable_threshold,
            )
        except Exception as exc:
            log.exception(
                "webhooks.dispatcher.rule_eval_failed",
                key=locator.file_key,
                error=str(exc),
            )


async def _consume_nowcasts(
    db: Database,
    http_client: httpx.AsyncClient,
    subscriber: NowcastGridSubscriber,
    auto_disable_threshold: int,
) -> None:
    """Fan out ``aeroza.nowcast.grids.new`` envelopes to opted-in subs.

    The wire payload is a plain dict (the JSON the nowcast publisher
    emits), so we forward it as ``data`` unchanged. No rule evaluation
    runs on nowcast events — alert rules evaluate against observation
    grids, not predictions. Webhook subscribers that want predictions
    delivered opt in via the ``events`` array.
    """
    async for envelope in subscriber.subscribe_new_nowcasts():
        try:
            await dispatch_event(
                db,
                http_client,
                event_type=NOWCAST_NEW_GRID_SUBJECT,
                event_data=envelope,
                auto_disable_threshold=auto_disable_threshold,
            )
        except Exception as exc:
            log.exception(
                "webhooks.dispatcher.nowcast_event_failed",
                envelope_id=envelope.get("id") if isinstance(envelope, dict) else None,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Raw fan-out


async def dispatch_event(
    db: Database,
    http_client: httpx.AsyncClient,
    *,
    event_type: str,
    event_data: dict[str, Any],
    auto_disable_threshold: int,
) -> None:
    """Deliver one event to every active subscription that wants it."""
    async with db.sessionmaker() as session:
        subs = list(await find_active_subscriptions_for_event(session, event_type))

    for sub in subs:
        request = DeliveryRequest(
            subscription_id=sub.id,
            rule_id=None,
            url=sub.url,
            secret=sub.secret,
            event_type=event_type,
            payload=_envelope(event_type, event_data),
        )
        async with db.sessionmaker() as session:
            outcome = await deliver_payload(session, request=request, http_client=http_client)
        await _handle_outcome(db, sub.id, outcome, auto_disable_threshold)


# ---------------------------------------------------------------------------
# Rule fan-out


async def evaluate_rules_for_grid(
    db: Database,
    http_client: httpx.AsyncClient,
    locator: MrmsGridLocator,
    *,
    auto_disable_threshold: int,
) -> None:
    """For each new MRMS grid: evaluate active rules; deliver on transition."""
    async with db.sessionmaker() as session:
        view = await find_mrms_grid_by_key(session, locator.file_key)
    if view is None:
        log.warning(
            "webhooks.dispatcher.grid_locator_without_catalog_row",
            key=locator.file_key,
        )
        return

    async with db.sessionmaker() as session:
        rules = list(await find_active_rules(session, product=view.product, level=view.level))

    point_sampler = _build_point_sampler(view.zarr_uri, view.variable)
    polygon_sampler = _build_polygon_sampler(view.zarr_uri, view.variable)

    for rule_row in rules:
        rule = alert_rule_from_row(rule_row)
        evaluation = await evaluate_rule(
            rule,
            point_sampler=point_sampler,
            polygon_sampler=polygon_sampler,
        )
        was_firing = rule_row.currently_firing
        is_firing_now = evaluation.predicate_satisfied
        fired_now = is_firing_now and not was_firing

        async with db.sessionmaker() as session:
            await update_rule_evaluation(
                session,
                rule.id,
                last_value=evaluation.value,
                currently_firing=is_firing_now,
                fired_now=fired_now,
            )
            await session.commit()

        if not fired_now:
            continue

        # Rule transitioned from not-firing to firing — deliver to its
        # bound subscription regardless of the subscription's `events`
        # array. Bound implies subscribed.
        async with db.sessionmaker() as session:
            sub = await get_subscription(session, rule_row.subscription_id)
        if sub is None or sub.status != "active":
            log.info(
                "webhooks.dispatcher.rule_fired_but_sub_inactive",
                rule_id=str(rule.id),
                subscription_id=str(rule_row.subscription_id),
                sub_status=getattr(sub, "status", None),
            )
            continue

        request = DeliveryRequest(
            subscription_id=sub.id,
            rule_id=rule.id,
            url=sub.url,
            secret=sub.secret,
            event_type=RULE_FIRED_EVENT,
            payload=_rule_fired_envelope(rule_row, evaluation, view),
        )
        async with db.sessionmaker() as session:
            outcome = await deliver_payload(session, request=request, http_client=http_client)
        await _handle_outcome(db, sub.id, outcome, auto_disable_threshold)


def _build_point_sampler(zarr_uri: str, variable: str) -> PointSampler:
    async def sampler(lat: float, lng: float, _p: str, _l: str) -> GridSample:
        return await sample_grid_at_point(
            zarr_uri=zarr_uri,
            variable=variable,
            latitude=lat,
            longitude=lng,
        )

    return sampler


def _build_polygon_sampler(zarr_uri: str, variable: str) -> PolygonSampler:
    async def sampler(
        polygon: str,
        reducer: str,
        _p: str,
        _l: str,
        threshold: str | None,
    ) -> PolygonSample:
        vertices = parse_polygon(polygon)
        if vertices is None:
            raise ValueError("polygon parser returned None for non-empty input")
        return await sample_grid_in_polygon(
            zarr_uri=zarr_uri,
            variable=variable,
            polygon_lng_lat=vertices,
            reducer=reducer,  # type: ignore[arg-type]
            threshold=float(threshold) if threshold is not None else None,
        )

    return sampler


# ---------------------------------------------------------------------------
# Circuit breaker


async def _handle_outcome(
    db: Database,
    subscription_id: uuid.UUID,
    outcome: DeliveryOutcome,
    threshold: int,
) -> None:
    """Mark the subscription disabled if the last ``threshold``
    deliveries all failed."""
    if not outcome.terminal_failure:
        return
    if threshold <= 0:
        return  # circuit-breaker disabled

    async with db.sessionmaker() as session:
        recent = await session.execute(
            select(WebhookDeliveryRow.status)
            .where(WebhookDeliveryRow.subscription_id == subscription_id)
            .where(WebhookDeliveryRow.status.in_(("ok", "failed")))
            .order_by(WebhookDeliveryRow.created_at.desc())
            .limit(threshold)
        )
        rows = list(recent.scalars().all())

    if len(rows) < threshold:
        return
    if any(status != "failed" for status in rows):
        return

    log.warning(
        "webhooks.dispatcher.auto_disable",
        subscription_id=str(subscription_id),
        threshold=threshold,
    )
    async with db.sessionmaker() as session:
        await update_subscription(
            session,
            subscription_id,
            WebhookSubscriptionPatch(status="disabled"),
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Wire envelopes


def _envelope(event_type: str, data: dict[str, Any]) -> dict[str, Any]:
    """Common payload shape: ``{event, deliveryId, data}``.

    ``deliveryId`` is generated client-side (per envelope) so a
    consumer can dedupe replays without parsing the signature
    timestamp. The dispatcher's per-attempt log row has its own UUID
    distinct from this one.
    """
    return {
        "type": "WebhookEvent",
        "event": event_type,
        "deliveryId": str(uuid.uuid4()),
        "data": data,
    }


def _rule_fired_envelope(
    rule_row: AlertRuleRow,
    evaluation: RuleEvaluation,
    view: Any,
) -> dict[str, Any]:
    rule = alert_rule_from_row(rule_row)
    return _envelope(
        RULE_FIRED_EVENT,
        {
            "rule": {
                "id": str(rule.id),
                "name": rule.name,
                "config": rule.config.model_dump(by_alias=True, mode="json"),
            },
            "evaluation": {
                "value": evaluation.value,
                "predicateSatisfied": evaluation.predicate_satisfied,
            },
            "grid": {
                "fileKey": view.file_key,
                "product": view.product,
                "level": view.level,
                "validAt": view.valid_at.isoformat() if view.valid_at else None,
            },
        },
    )


def _file_to_dict(file: MrmsFile) -> dict[str, Any]:
    return {
        "key": file.key,
        "product": file.product,
        "level": file.level,
        "validAt": file.valid_at.isoformat(),
        "sizeBytes": file.size_bytes,
        "etag": file.etag,
    }


def _locator_to_dict(loc: MrmsGridLocator) -> dict[str, Any]:
    return {
        "fileKey": loc.file_key,
        "zarrUri": loc.zarr_uri,
        "variable": loc.variable,
        "dims": list(loc.dims),
        "shape": list(loc.shape),
        "dtype": loc.dtype,
        "nbytes": loc.nbytes,
    }


__all__ = [
    "AUTO_DISABLE_CONSECUTIVE_FAILURES",
    "RULE_FIRED_EVENT",
    "dispatch_event",
    "evaluate_rules_for_grid",
    "run_dispatcher",
]
