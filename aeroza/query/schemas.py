"""Wire-format schemas for query endpoints.

Output is always GeoJSON-shaped — alerts are inherently geographic and every
mapping client (MapLibre, Mapbox, Leaflet) consumes ``FeatureCollection``
natively. Field names use the NWS-style camelCase (``areaDesc``, ``senderName``)
so consumers can compare or merge with the upstream NWS API without renaming.

The list endpoint emits :class:`AlertProperties` (no long-form prose); the
detail endpoint emits :class:`AlertDetailProperties` which adds
``description`` and ``instruction``. Splitting the schemas keeps list payloads
tight and lets clients tell list vs detail apart at the type level.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from aeroza.query.alerts import AlertView

GEOJSON_FEATURE_COLLECTION_TYPE: Final[Literal["FeatureCollection"]] = "FeatureCollection"
GEOJSON_FEATURE_TYPE: Final[Literal["Feature"]] = "Feature"


class AlertProperties(BaseModel):
    """Per-alert metadata for the list endpoint.

    Excludes ``description`` and ``instruction`` to keep list payloads small;
    callers wanting the full record should use ``GET /v1/alerts/{id}``.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    id: str
    event: str
    headline: str | None = None
    severity: str
    urgency: str
    certainty: str
    sender_name: str | None = Field(default=None, serialization_alias="senderName")
    area_desc: str | None = Field(default=None, serialization_alias="areaDesc")
    effective: datetime | None = None
    onset: datetime | None = None
    expires: datetime | None = None
    ends: datetime | None = None


class AlertDetailProperties(AlertProperties):
    """Per-alert metadata for the detail endpoint, including long-form prose."""

    description: str | None = None
    instruction: str | None = None


class AlertFeature(BaseModel):
    type: Literal["Feature"] = GEOJSON_FEATURE_TYPE
    geometry: dict[str, Any] | None = None
    properties: AlertProperties


class AlertDetailFeature(BaseModel):
    type: Literal["Feature"] = GEOJSON_FEATURE_TYPE
    geometry: dict[str, Any] | None = None
    properties: AlertDetailProperties


class AlertFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = GEOJSON_FEATURE_COLLECTION_TYPE
    features: list[AlertFeature]


def alert_view_to_feature(view: AlertView) -> AlertFeature:
    return AlertFeature(
        geometry=view.geometry,
        properties=AlertProperties(
            id=view.id,
            event=view.event,
            headline=view.headline,
            severity=view.severity,
            urgency=view.urgency,
            certainty=view.certainty,
            sender_name=view.sender_name,
            area_desc=view.area_desc,
            effective=view.effective,
            onset=view.onset,
            expires=view.expires,
            ends=view.ends,
        ),
    )


def alert_view_to_detail_feature(view: AlertView) -> AlertDetailFeature:
    return AlertDetailFeature(
        geometry=view.geometry,
        properties=AlertDetailProperties(
            id=view.id,
            event=view.event,
            headline=view.headline,
            description=view.description,
            instruction=view.instruction,
            severity=view.severity,
            urgency=view.urgency,
            certainty=view.certainty,
            sender_name=view.sender_name,
            area_desc=view.area_desc,
            effective=view.effective,
            onset=view.onset,
            expires=view.expires,
            ends=view.ends,
        ),
    )
