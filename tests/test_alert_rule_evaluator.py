"""Unit tests for the pure rule evaluator.

The evaluator is a small, side-effect-free function: given a rule and
a way to read the grid, return whether the predicate held. Tests
inject fake samplers so we can pin every branch (point/polygon, every
predicate op, error fall-through) without DB or Zarr.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from aeroza.query.mrms_sample import GridSample, OutOfDomainError, PolygonSample
from aeroza.webhooks.rule_evaluator import (
    PointSampler,
    PolygonSampler,
    evaluate_rule,
    predicate_holds,
)
from aeroza.webhooks.rule_schemas import (
    AlertRule,
    PointRuleConfig,
    PolygonRuleConfig,
    Predicate,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures


def _alert_rule(
    *,
    config: PointRuleConfig | PolygonRuleConfig,
    status: str = "active",
    currently_firing: bool = False,
) -> AlertRule:
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    return AlertRule(
        id=uuid4(),
        subscription_id=uuid4(),
        name="t",
        description=None,
        config=config,
        status=status,  # type: ignore[arg-type]
        currently_firing=currently_firing,
        last_value=None,
        last_evaluated_at=None,
        last_fired_at=None,
        created_at=now,
        updated_at=now,
    )


def _grid_sample(value: float) -> GridSample:
    return GridSample(value=value, latitude=29.8, longitude=-95.4, variable="reflectivity")


def _polygon_sample(value: float) -> PolygonSample:
    return PolygonSample(
        reducer="max",
        value=value,
        cell_count=10,
        variable="reflectivity",
        threshold=None,
        bbox_min_latitude=29.5,
        bbox_min_longitude=-95.7,
        bbox_max_latitude=30.0,
        bbox_max_longitude=-95.0,
    )


def _stub_point(returned_value: float) -> PointSampler:
    async def stub(lat: float, lng: float, product: str, level: str) -> GridSample:
        return _grid_sample(returned_value)

    return stub


def _stub_polygon(returned_value: float) -> PolygonSampler:
    async def stub(
        polygon: str, reducer: str, product: str, level: str, threshold: str | None
    ) -> PolygonSample:
        return _polygon_sample(returned_value)

    return stub


def _failing_polygon() -> PolygonSampler:
    async def _never_called(
        polygon: str, reducer: str, product: str, level: str, threshold: str | None
    ) -> PolygonSample:
        raise AssertionError("polygon sampler should not be called for a point rule")

    return _never_called


def _failing_point() -> PointSampler:
    async def _never_called(lat: float, lng: float, product: str, level: str) -> GridSample:
        raise AssertionError("point sampler should not be called for a polygon rule")

    return _never_called


# ---------------------------------------------------------------------------
# predicate_holds


@pytest.mark.parametrize(
    ("op", "value", "threshold", "expected"),
    [
        (">", 41.0, 40.0, True),
        (">", 40.0, 40.0, False),
        (">=", 40.0, 40.0, True),
        ("<", 39.0, 40.0, True),
        ("<", 40.0, 40.0, False),
        ("<=", 40.0, 40.0, True),
        ("==", 40.0, 40.0, True),
        ("==", 40.0001, 40.0, False),
        ("!=", 40.0, 40.0, False),
        ("!=", 40.0001, 40.0, True),
    ],
)
def test_predicate_holds_covers_every_op(
    op: str, value: float, threshold: float, expected: bool
) -> None:
    p = Predicate(op=op, threshold=threshold)  # type: ignore[arg-type]
    assert predicate_holds(value, p) is expected


# ---------------------------------------------------------------------------
# evaluate_rule — point


async def test_evaluate_point_predicate_satisfied() -> None:
    rule = _alert_rule(
        config=PointRuleConfig(
            type="point",
            lat=29.76,
            lng=-95.37,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )
    result = await evaluate_rule(
        rule,
        point_sampler=_stub_point(42.0),
        polygon_sampler=_failing_polygon(),
    )
    assert result.value == 42.0
    assert result.predicate_satisfied is True
    assert result.error_reason is None


async def test_evaluate_point_predicate_not_satisfied() -> None:
    rule = _alert_rule(
        config=PointRuleConfig(
            type="point",
            lat=0,
            lng=0,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )
    result = await evaluate_rule(
        rule,
        point_sampler=_stub_point(35.0),
        polygon_sampler=_failing_polygon(),
    )
    assert result.value == 35.0
    assert result.predicate_satisfied is False


async def test_evaluate_point_out_of_domain_returns_none_value() -> None:
    rule = _alert_rule(
        config=PointRuleConfig(
            type="point",
            lat=50.0,
            lng=-50.0,
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )

    async def oob_sampler(*_args: object) -> GridSample:
        raise OutOfDomainError("no cell within 0.05° of (lat=50, lng=-50)")

    result = await evaluate_rule(
        rule,
        point_sampler=oob_sampler,
        polygon_sampler=_failing_polygon(),
    )
    assert result.value is None
    assert result.predicate_satisfied is False
    assert result.error_reason is not None
    assert "no cell" in result.error_reason


async def test_evaluate_point_unexpected_exception_is_swallowed() -> None:
    """Raw exceptions become an error_reason — slice 3's dispatcher
    counts these toward "disable after sustained failures" without
    crashing the worker."""
    rule = _alert_rule(
        config=PointRuleConfig(
            type="point",
            lat=0,
            lng=0,
            predicate=Predicate(op=">", threshold=0.0),
        ),
    )

    async def boom(*_args: object) -> GridSample:
        raise RuntimeError("zarr store missing")

    result = await evaluate_rule(
        rule,
        point_sampler=boom,
        polygon_sampler=_failing_polygon(),
    )
    assert result.value is None
    assert result.predicate_satisfied is False
    assert result.error_reason == "zarr store missing"


# ---------------------------------------------------------------------------
# evaluate_rule — polygon


async def test_evaluate_polygon_predicate_satisfied() -> None:
    rule = _alert_rule(
        config=PolygonRuleConfig(
            type="polygon",
            polygon="-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
            reducer="max",
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )
    result = await evaluate_rule(
        rule,
        point_sampler=_failing_point(),
        polygon_sampler=_stub_polygon(55.0),
    )
    assert result.value == 55.0
    assert result.predicate_satisfied is True


async def test_evaluate_polygon_count_ge_passes_threshold_to_sampler() -> None:
    """``count_ge``'s threshold must reach the polygon sampler — verified
    by capturing the args the sampler receives."""
    rule = _alert_rule(
        config=PolygonRuleConfig(
            type="polygon",
            polygon="-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
            reducer="count_ge",
            countThreshold=40.0,
            predicate=Predicate(op=">=", threshold=5.0),
        ),
    )
    captured: dict[str, object] = {}

    async def capturing_sampler(
        polygon: str, reducer: str, product: str, level: str, threshold: str | None
    ) -> PolygonSample:
        captured.update(
            polygon=polygon, reducer=reducer, product=product, level=level, threshold=threshold
        )
        return _polygon_sample(17.0)  # 17 cells >= threshold

    result = await evaluate_rule(
        rule,
        point_sampler=_failing_point(),
        polygon_sampler=capturing_sampler,
    )
    assert captured["reducer"] == "count_ge"
    assert captured["threshold"] == "40.0"
    assert result.value == 17.0
    assert result.predicate_satisfied is True


async def test_evaluate_polygon_out_of_domain_returns_none_value() -> None:
    rule = _alert_rule(
        config=PolygonRuleConfig(
            type="polygon",
            polygon="10,50,11,50,11,51,10,51",
            reducer="max",
            predicate=Predicate(op=">=", threshold=40.0),
        ),
    )

    async def oob_sampler(*_args: object) -> PolygonSample:
        raise OutOfDomainError("polygon outside grid")

    result = await evaluate_rule(
        rule,
        point_sampler=_failing_point(),
        polygon_sampler=oob_sampler,
    )
    assert result.value is None
    assert result.predicate_satisfied is False
    assert result.error_reason == "polygon outside grid"
