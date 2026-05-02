"""Unit tests for the alert-rule pydantic schemas.

The schemas are the API boundary's first line of defence — every test
here pins a wire-shape contract or a validation rule that the route
+ store layers depend on.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from aeroza.webhooks.rule_schemas import (
    AlertRuleCreate,
    AlertRulePatch,
    PointRuleConfig,
    PolygonRuleConfig,
    Predicate,
    config_to_jsonb,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Predicate


def test_predicate_round_trips_op_and_threshold() -> None:
    p = Predicate(op=">=", threshold=40.0)
    assert p.op == ">="
    assert p.threshold == 40.0


@pytest.mark.parametrize("op", [">", ">=", "<", "<=", "==", "!="])
def test_predicate_accepts_every_op(op: str) -> None:
    Predicate(op=op, threshold=0.0)  # type: ignore[arg-type]


def test_predicate_rejects_unknown_op() -> None:
    with pytest.raises(ValidationError):
        Predicate(op="approximately", threshold=0.0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Point


def test_point_config_round_trip() -> None:
    cfg = PointRuleConfig(
        type="point",
        lat=29.76,
        lng=-95.37,
        product="MergedReflectivityComposite",
        level="00.50",
        predicate=Predicate(op=">=", threshold=40.0),
    )
    assert cfg.type == "point"
    assert cfg.lat == 29.76
    assert cfg.predicate.threshold == 40.0


def test_point_config_defaults_product_and_level() -> None:
    cfg = PointRuleConfig(type="point", lat=0, lng=0, predicate=Predicate(op=">", threshold=1.0))
    assert cfg.product == "MergedReflectivityComposite"
    assert cfg.level == "00.50"


@pytest.mark.parametrize(
    ("lat", "lng"),
    [(91, 0), (-91, 0), (0, 181), (0, -181)],
)
def test_point_config_rejects_out_of_range(lat: float, lng: float) -> None:
    with pytest.raises(ValidationError):
        PointRuleConfig(
            type="point",
            lat=lat,
            lng=lng,
            predicate=Predicate(op=">", threshold=0.0),
        )


# ---------------------------------------------------------------------------
# Polygon


def _polygon_config(**overrides: object) -> PolygonRuleConfig:
    base = {
        "type": "polygon",
        "polygon": "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
        "reducer": "max",
        "predicate": Predicate(op=">=", threshold=40.0),
    }
    base.update(overrides)
    return PolygonRuleConfig.model_validate(base)


def test_polygon_config_accepts_well_formed_polygon() -> None:
    cfg = _polygon_config()
    assert cfg.reducer == "max"
    assert "29.5" in cfg.polygon


@pytest.mark.parametrize(
    "bad_polygon",
    [
        "1,2",  # too few vertices
        "1,2,3,4",  # 2 vertices
        "a,b,c,d,e,f",  # non-numeric
        "1,2,3,4,5",  # odd component count
        "",
    ],
)
def test_polygon_config_rejects_malformed_polygon(bad_polygon: str) -> None:
    with pytest.raises(ValidationError):
        _polygon_config(polygon=bad_polygon)


def test_polygon_count_ge_requires_threshold() -> None:
    with pytest.raises(ValidationError, match="countThreshold"):
        _polygon_config(reducer="count_ge")


def test_polygon_count_ge_with_threshold_validates() -> None:
    cfg = _polygon_config(reducer="count_ge", countThreshold=40.0)
    assert cfg.reducer == "count_ge"
    assert cfg.count_threshold == 40.0


# ---------------------------------------------------------------------------
# AlertRuleCreate (the wire envelope)


def test_create_envelope_validates_full_point_rule() -> None:
    sub_id = uuid4()
    payload = AlertRuleCreate.model_validate(
        {
            "subscriptionId": str(sub_id),
            "name": "Houston reflectivity ≥ 40",
            "config": {
                "type": "point",
                "lat": 29.76,
                "lng": -95.37,
                "predicate": {"op": ">=", "threshold": 40.0},
            },
        }
    )
    assert payload.subscription_id == sub_id
    assert isinstance(payload.config, PointRuleConfig)
    assert payload.config.lat == 29.76


def test_create_envelope_dispatches_polygon_via_discriminator() -> None:
    payload = AlertRuleCreate.model_validate(
        {
            "subscriptionId": str(uuid4()),
            "name": "Region max ≥ 40",
            "config": {
                "type": "polygon",
                "polygon": "-95.7,29.5,-95.0,29.5,-95.0,30.0,-95.7,30.0",
                "reducer": "max",
                "predicate": {"op": ">=", "threshold": 40.0},
            },
        }
    )
    assert isinstance(payload.config, PolygonRuleConfig)
    assert payload.config.polygon.endswith("30.0")


def test_create_envelope_rejects_missing_discriminator() -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate.model_validate(
            {
                "subscriptionId": str(uuid4()),
                "name": "x",
                "config": {
                    # no "type"
                    "lat": 0,
                    "lng": 0,
                    "predicate": {"op": ">", "threshold": 0.0},
                },
            }
        )


def test_create_envelope_rejects_blank_name() -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate.model_validate(
            {
                "subscriptionId": str(uuid4()),
                "name": "",
                "config": {
                    "type": "point",
                    "lat": 0,
                    "lng": 0,
                    "predicate": {"op": ">", "threshold": 0.0},
                },
            }
        )


# ---------------------------------------------------------------------------
# AlertRulePatch — every field optional, status validated


def test_patch_accepts_partial_update() -> None:
    patch = AlertRulePatch(name="renamed")
    assert patch.name == "renamed"
    assert patch.config is None
    assert patch.status is None


def test_patch_rejects_unknown_status() -> None:
    with pytest.raises(ValidationError):
        AlertRulePatch.model_validate({"status": "explosive"})


# ---------------------------------------------------------------------------
# config_to_jsonb — strips the discriminator before persistence


def test_config_to_jsonb_drops_type_discriminator() -> None:
    cfg = PointRuleConfig(type="point", lat=0, lng=0, predicate=Predicate(op=">", threshold=0.0))
    payload = config_to_jsonb(cfg)
    assert "type" not in payload
    assert payload["lat"] == 0
    assert payload["predicate"]["op"] == ">"
