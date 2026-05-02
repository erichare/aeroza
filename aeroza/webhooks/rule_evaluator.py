"""Pure evaluator for alert rules.

Given an :class:`AlertRule` and a way to read grid values, returns a
:class:`RuleEvaluation` describing whether the predicate held this
tick. The dispatcher (slice 3) is the only call site; this module is
deliberately I/O-free so the predicate logic is unit-testable without
NATS / Postgres / Zarr.

Two surfaces:

- :func:`evaluate_rule` — async, takes async callbacks for the two
  underlying readers (point sample, polygon reduce). Mirrors the
  signature pattern of :mod:`aeroza.query.mrms_sample`.
- :func:`predicate_holds` — sync, pure scalar comparison. Exposed
  for tests and for any future caller that wants to evaluate a
  predicate against a value it already has.

The evaluator does **not** know about firing-state transitions. The
dispatcher reads ``rule.currently_firing`` from the row and decides
whether the new ``predicate_satisfied`` constitutes a fresh fire.
Keeping that logic in the dispatcher means the evaluator stays a
pure function.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import structlog

from aeroza.query.mrms_sample import GridSample, OutOfDomainError, PolygonSample
from aeroza.webhooks.rule_schemas import (
    AlertRule,
    PointRuleConfig,
    PolygonRuleConfig,
    Predicate,
    PredicateOp,
    RuleConfig,
)

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RuleEvaluation:
    """One evaluator output. Always populated; always exception-free.

    ``value`` is the scalar the predicate was checked against (the
    sample at the point, or the reducer's output over the polygon).
    ``None`` means the read errored — see ``error_reason``.

    ``predicate_satisfied`` reflects whether the predicate held against
    ``value``. ``False`` when ``value is None`` (an erroring read can't
    satisfy a predicate).

    ``error_reason`` carries the human-readable detail for the
    operator UI when something went wrong (out-of-domain coords, bad
    config, missing variable). The dispatcher logs the reason and
    decides whether to disable the rule after sustained errors.
    """

    value: float | None
    predicate_satisfied: bool
    error_reason: str | None = None


# Type aliases for the callbacks the dispatcher injects. Kept as plain
# types (no Protocol) because the surface is two functions and naming
# them with one alias each is clearer than another class.
PointSampler = Callable[[float, float, str, str], Awaitable[GridSample]]
PolygonSampler = Callable[
    [str, str, str, str, str | None],  # polygon, reducer, product, level, threshold-as-str
    Awaitable[PolygonSample],
]


_OPS: Final[dict[PredicateOp, Callable[[float, float], bool]]] = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def predicate_holds(value: float, predicate: Predicate) -> bool:
    """Return ``value op predicate.threshold``. Pure; no I/O."""
    op_fn = _OPS.get(predicate.op)
    if op_fn is None:
        raise ValueError(f"unknown predicate op {predicate.op!r}")
    return op_fn(value, predicate.threshold)


async def evaluate_rule(
    rule: AlertRule,
    *,
    point_sampler: PointSampler,
    polygon_sampler: PolygonSampler,
) -> RuleEvaluation:
    """Read the grid and check the rule's predicate.

    The dispatcher passes async callbacks that bind a Zarr URI / DB
    session at call time; this function only knows about the rule's
    config. Errors from the readers (out-of-domain, missing variable,
    Zarr open failure) are caught and surfaced via
    :class:`RuleEvaluation`'s ``error_reason``.
    """
    config = rule.config
    try:
        if isinstance(config, PointRuleConfig):
            value = await _evaluate_point(config, point_sampler)
        elif isinstance(config, PolygonRuleConfig):
            value = await _evaluate_polygon(config, polygon_sampler)
        else:
            # Should be unreachable via the discriminated union; defensive.
            raise TypeError(f"unhandled rule config type: {type(config).__name__}")
    except OutOfDomainError as exc:
        log.info(
            "alert_rules.evaluate.out_of_domain",
            rule_id=str(rule.id),
            reason=str(exc),
        )
        return RuleEvaluation(value=None, predicate_satisfied=False, error_reason=str(exc))
    except Exception as exc:
        log.exception(
            "alert_rules.evaluate.failed",
            rule_id=str(rule.id),
            error=str(exc),
        )
        return RuleEvaluation(value=None, predicate_satisfied=False, error_reason=str(exc))

    held = predicate_holds(value, _predicate(config))
    log.debug(
        "alert_rules.evaluate.tick",
        rule_id=str(rule.id),
        value=value,
        predicate_op=_predicate(config).op,
        threshold=_predicate(config).threshold,
        satisfied=held,
    )
    return RuleEvaluation(value=value, predicate_satisfied=held)


async def _evaluate_point(config: PointRuleConfig, sampler: PointSampler) -> float:
    sample = await sampler(config.lat, config.lng, config.product, config.level)
    return sample.value


async def _evaluate_polygon(config: PolygonRuleConfig, sampler: PolygonSampler) -> float:
    threshold = str(config.count_threshold) if config.count_threshold is not None else None
    sample = await sampler(
        config.polygon,
        config.reducer,
        config.product,
        config.level,
        threshold,
    )
    return sample.value


def _predicate(config: RuleConfig) -> Predicate:
    return config.predicate


__all__ = [
    "PointSampler",
    "PolygonSampler",
    "RuleEvaluation",
    "evaluate_rule",
    "predicate_holds",
]
