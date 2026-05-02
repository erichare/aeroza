"""Wire shapes for alert rules.

Two rule types share one envelope:

- :class:`PointRuleConfig` — predicate over the value sampled at a
  single (lat, lng).
- :class:`PolygonRuleConfig` — predicate over a reducer's output
  (max / mean / min / count_ge) over the cells inside a polygon.

Both carry a :class:`Predicate` (op + threshold) and the same
``product``/``level`` fields. Pydantic's discriminated union over
``type`` validates the right shape on the wire; the DB stores the
config as JSONB.

The wire is camelCase; the JSON stored in ``config`` keeps the
camelCase keys so the round-trip through Postgres is verbatim.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Any, Final, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from aeroza.webhooks.rule_models import ALERT_RULE_STATUSES

AlertRuleStatus = Literal["active", "paused", "disabled"]
AlertRuleType = Literal["point", "polygon"]
PolygonReducer = Literal["max", "mean", "min", "count_ge"]
PredicateOp = Literal[">", ">=", "<", "<=", "==", "!="]

_NAME_MAX_LEN: Final[int] = 128
_DESCRIPTION_MAX_LEN: Final[int] = 512
# Defaults match the rest of the API surface. Stored on every rule so a
# product roll-out (e.g. switching from MergedReflectivityComposite to
# something else) doesn't silently re-target every existing rule.
_DEFAULT_PRODUCT: Final[str] = "MergedReflectivityComposite"
_DEFAULT_LEVEL: Final[str] = "00.50"


class Predicate(BaseModel):
    """``value op threshold``. ``op`` is a comparator; ``threshold`` is float.

    Equality (``==``/``!=``) is legal but rarely useful for floating-point
    grid samples — included for completeness so the dispatcher can
    treat the operator set as a closed enum.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    op: PredicateOp
    threshold: float


class _RuleBaseConfig(BaseModel):
    """Fields shared by every rule type. Concrete configs add their own."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    product: str = Field(default=_DEFAULT_PRODUCT)
    level: str = Field(default=_DEFAULT_LEVEL)
    predicate: Predicate


class PointRuleConfig(_RuleBaseConfig):
    """Predicate over the value sampled at a (lat, lng)."""

    type: Literal["point"] = "point"
    lat: float = Field(..., ge=-90.0, le=90.0)
    lng: float = Field(..., ge=-180.0, le=180.0)


_POLYGON_RE: Final[re.Pattern[str]] = re.compile(r"^-?\d+(\.\d+)?(,-?\d+(\.\d+)?)+$")


class PolygonRuleConfig(_RuleBaseConfig):
    """Predicate over a polygon reducer.

    ``polygon`` matches the format the ``/v1/mrms/grids/polygon`` route
    accepts: flat ``"lng,lat,lng,lat,…"`` with ≥3 vertices, ring
    implicitly closed. Validation here pre-rejects obviously malformed
    strings; the route's :func:`parse_polygon` is the canonical parser
    and runs again at evaluation time.
    """

    type: Literal["polygon"] = "polygon"
    polygon: str
    reducer: PolygonReducer = "max"
    # Required when ``reducer == "count_ge"``. The evaluator double-
    # checks; this validator is the API boundary's first line of defence.
    count_threshold: float | None = Field(default=None, alias="countThreshold")

    @field_validator("polygon")
    @classmethod
    def _validate_polygon_shape(cls, value: str) -> str:
        if not _POLYGON_RE.match(value):
            raise ValueError(
                "polygon must be 'lng,lat,lng,lat,...' (decimal numbers, "
                "≥6 components for ≥3 vertices)"
            )
        # Quick check: at least three vertices (six numbers).
        parts = value.split(",")
        if len(parts) < 6 or len(parts) % 2 != 0:
            raise ValueError(
                "polygon needs at least 3 vertices (6 numbers) and must have an even count"
            )
        return value

    @model_validator(mode="after")
    def _require_threshold_for_count_ge(self) -> PolygonRuleConfig:
        if self.reducer == "count_ge" and self.count_threshold is None:
            raise ValueError("polygon rule with reducer='count_ge' requires countThreshold")
        return self


# Discriminated union — pydantic picks the right concrete config off
# the ``type`` field. Both shapes pickle through JSONB cleanly.
RuleConfig = Annotated[
    PointRuleConfig | PolygonRuleConfig,
    Field(discriminator="type"),
]


class AlertRuleCreate(BaseModel):
    """Request body for ``POST /v1/alert-rules``."""

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    subscription_id: UUID = Field(alias="subscriptionId")
    name: str = Field(..., min_length=1, max_length=_NAME_MAX_LEN)
    description: str | None = Field(default=None, max_length=_DESCRIPTION_MAX_LEN)
    config: RuleConfig


class AlertRulePatch(BaseModel):
    """Request body for ``PATCH /v1/alert-rules/{id}``.

    Every field is optional. ``config`` is replaced wholesale —
    sub-field deltas aren't supported on the wire (small enough to
    read/edit/write).
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    name: str | None = Field(default=None, min_length=1, max_length=_NAME_MAX_LEN)
    description: str | None = Field(default=None, max_length=_DESCRIPTION_MAX_LEN)
    config: RuleConfig | None = None
    status: AlertRuleStatus | None = None

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in ALERT_RULE_STATUSES:
            raise ValueError(f"status must be one of {list(ALERT_RULE_STATUSES)}")
        return value


class AlertRule(BaseModel):
    """Response shape for every read endpoint.

    Mirrors the row 1:1 — there's no secret-redaction analog here,
    so a single response model covers create / list / get / patch.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["AlertRule"] = "AlertRule"
    id: UUID
    subscription_id: UUID = Field(serialization_alias="subscriptionId")
    name: str
    description: str | None
    config: RuleConfig
    status: AlertRuleStatus
    currently_firing: bool = Field(serialization_alias="currentlyFiring")
    last_value: float | None = Field(serialization_alias="lastValue")
    last_evaluated_at: datetime | None = Field(serialization_alias="lastEvaluatedAt")
    last_fired_at: datetime | None = Field(serialization_alias="lastFiredAt")
    created_at: datetime = Field(serialization_alias="createdAt")
    updated_at: datetime = Field(serialization_alias="updatedAt")


class AlertRuleList(BaseModel):
    """Envelope for ``GET /v1/alert-rules``."""

    type: Literal["AlertRuleList"] = "AlertRuleList"
    items: list[AlertRule]


def alert_rule_from_row(row: Any) -> AlertRule:
    """Project an :class:`AlertRuleRow` into the wire shape.

    The row's ``rule_type`` discriminator is collapsed into the JSONB
    config's ``type`` field for the wire model, so the response carries
    one ``config.type`` discriminator (matching :data:`RuleConfig`'s
    union) instead of two.
    """
    config_dict: dict[str, Any] = dict(row.config)
    config_dict.setdefault("type", row.rule_type)
    return AlertRule(
        id=row.id,
        subscription_id=row.subscription_id,
        name=row.name,
        description=row.description,
        config=config_dict,  # type: ignore[arg-type]
        status=row.status,
        currently_firing=row.currently_firing,
        last_value=row.last_value,
        last_evaluated_at=row.last_evaluated_at,
        last_fired_at=row.last_fired_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def config_to_jsonb(config: RuleConfig) -> dict[str, Any]:
    """Serialise a config for storage. Strips the discriminator from
    the JSONB payload — the discriminator already lives in the row's
    ``rule_type`` column and we don't want it in two places."""
    payload = config.model_dump(by_alias=True, exclude_none=False)
    payload.pop("type", None)
    return payload


__all__ = [
    "AlertRule",
    "AlertRuleCreate",
    "AlertRuleList",
    "AlertRulePatch",
    "AlertRuleStatus",
    "AlertRuleType",
    "PointRuleConfig",
    "PolygonReducer",
    "PolygonRuleConfig",
    "Predicate",
    "PredicateOp",
    "RuleConfig",
    "alert_rule_from_row",
    "config_to_jsonb",
]
