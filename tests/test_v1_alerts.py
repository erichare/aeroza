"""End-to-end integration tests for ``GET /v1/alerts``.

Seeds the database via ``upsert_alerts`` then exercises the route through
``httpx.AsyncClient`` against an in-process ASGI app whose lifespan is
short-circuited by setting ``app.state.db`` directly to the test database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.nws_alerts import Alert, Certainty, Severity, Urgency
from aeroza.ingest.nws_alerts_store import upsert_alerts
from aeroza.shared.db import Database

pytestmark = pytest.mark.integration

ROUTE: str = "/v1/alerts"
_DEFAULT_POLYGON_SENTINEL: object = object()


def _polygon(min_lng: float, min_lat: float, max_lng: float, max_lat: float) -> dict[str, Any]:
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [min_lng, min_lat],
                [min_lng, max_lat],
                [max_lng, max_lat],
                [max_lng, min_lat],
                [min_lng, min_lat],
            ]
        ],
    }


def _alert(
    alert_id: str,
    *,
    severity: Severity = Severity.SEVERE,
    expires_offset: timedelta = timedelta(hours=1),
    geometry: dict[str, Any] | None | object = _DEFAULT_POLYGON_SENTINEL,
    event: str = "Severe Thunderstorm Warning",
    description: str | None = None,
    instruction: str | None = None,
) -> Alert:
    now = datetime.now(UTC)
    if geometry is _DEFAULT_POLYGON_SENTINEL:
        actual_geometry: dict[str, Any] | None = _polygon(-95.7, 29.5, -95.0, 30.0)
    else:
        # Explicit None or explicit dict — pass through verbatim.
        actual_geometry = geometry  # type: ignore[assignment]
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": event,
            "headline": f"{event} for {alert_id}",
            "description": description,
            "instruction": instruction,
            "severity": severity,
            "urgency": Urgency.IMMEDIATE,
            "certainty": Certainty.OBSERVED,
            "sender_name": "NWS Test",
            "area_desc": "Test Area",
            "effective": now,
            "onset": now,
            "expires": now + expires_offset,
            "ends": now + expires_offset,
            "geometry": actual_geometry,
        }
    )


async def _seed(integration_db: Database, *alerts: Alert) -> None:
    async with integration_db.sessionmaker() as session:
        await upsert_alerts(session, alerts)
        await session.commit()


async def test_returns_empty_feature_collection_when_no_alerts(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body == {"type": "FeatureCollection", "features": []}


async def test_returns_seeded_alerts_as_geojson(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(integration_db, _alert("a1"))
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1
    feature = body["features"][0]
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Polygon"
    props = feature["properties"]
    assert props["id"] == "a1"
    assert props["severity"] == "Severe"
    assert props["areaDesc"] == "Test Area"  # alias preserved on the wire
    assert props["senderName"] == "NWS Test"


async def test_excludes_expired_alerts(api_client: AsyncClient, integration_db: Database) -> None:
    await _seed(
        integration_db,
        _alert("active", expires_offset=timedelta(hours=1)),
        _alert("expired", expires_offset=timedelta(hours=-1)),
    )
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["active"]


async def test_point_filter_intersects_polygon(
    api_client: AsyncClient, integration_db: Database
) -> None:
    inside_polygon = _polygon(-95.7, 29.5, -95.0, 30.0)
    outside_polygon = _polygon(-100.0, 40.0, -99.0, 41.0)
    await _seed(
        integration_db,
        _alert("inside", geometry=inside_polygon),
        _alert("outside", geometry=outside_polygon),
    )
    response = await api_client.get(ROUTE, params={"point": "29.76,-95.37"})
    assert response.status_code == 200
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["inside"]


async def test_severity_filter_excludes_lower_levels(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _alert("ext", severity=Severity.EXTREME),
        _alert("sev", severity=Severity.SEVERE),
        _alert("mod", severity=Severity.MODERATE),
        _alert("min", severity=Severity.MINOR),
    )
    response = await api_client.get(ROUTE, params={"severity": "Severe"})
    assert response.status_code == 200
    ids = {feature["properties"]["id"] for feature in response.json()["features"]}
    assert ids == {"ext", "sev"}


async def test_results_ordered_by_severity_descending(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _alert("mod", severity=Severity.MODERATE),
        _alert("ext", severity=Severity.EXTREME),
        _alert("sev", severity=Severity.SEVERE),
    )
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["ext", "sev", "mod"]


async def test_limit_clamps_results(api_client: AsyncClient, integration_db: Database) -> None:
    await _seed(integration_db, *(_alert(f"a{i}") for i in range(5)))
    response = await api_client.get(ROUTE, params={"limit": 2})
    assert response.status_code == 200
    assert len(response.json()["features"]) == 2


async def test_invalid_point_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"point": "not-a-coord"})
    assert response.status_code == 400
    assert "invalid point" in response.json()["detail"]


async def test_out_of_range_lat_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"point": "100,0"})
    assert response.status_code == 400
    assert "latitude" in response.json()["detail"]


async def test_invalid_severity_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"severity": "Catastrophic"})
    assert response.status_code == 422


async def test_limit_above_max_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"limit": 9999})
    assert response.status_code == 422


async def test_alert_without_geometry_excluded_from_point_query(
    api_client: AsyncClient, integration_db: Database
) -> None:
    no_geom = _alert("nogeom", geometry=None)
    # Sanity: also seed an alert with geometry so we know the route returns it.
    await _seed(integration_db, no_geom, _alert("withgeom"))
    response = await api_client.get(ROUTE, params={"point": "29.76,-95.37"})
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["withgeom"]


async def test_503_when_db_state_missing(api_client: AsyncClient) -> None:
    """Drop ``app.state.db`` and confirm the dependency surfaces a 503.

    Restores the database before yielding back so the post-test TRUNCATE
    fixture finalizer still works.
    """
    app = api_client._transport.app  # type: ignore[attr-defined]
    saved = app.state.db
    delattr(app.state, "db")
    try:
        response = await api_client.get(ROUTE)
        assert response.status_code == 503
        assert response.json()["detail"] == "database not available"
    finally:
        app.state.db = saved
        async with saved.sessionmaker() as session:
            await session.execute(text("SELECT 1"))


# --------------------------------------------------------------------------- #
# bbox filter — list endpoint                                                  #
# --------------------------------------------------------------------------- #


async def test_bbox_filter_returns_overlapping_polygons(
    api_client: AsyncClient, integration_db: Database
) -> None:
    inside_polygon = _polygon(-95.7, 29.5, -95.0, 30.0)
    outside_polygon = _polygon(-100.0, 40.0, -99.0, 41.0)
    await _seed(
        integration_db,
        _alert("inside", geometry=inside_polygon),
        _alert("outside", geometry=outside_polygon),
    )
    response = await api_client.get(ROUTE, params={"bbox": "-96.0,29.0,-94.0,31.0"})
    assert response.status_code == 200
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["inside"]


async def test_bbox_filter_excludes_alert_with_null_geometry(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _alert("nogeom", geometry=None),
        _alert("withgeom"),
    )
    response = await api_client.get(ROUTE, params={"bbox": "-96.0,29.0,-94.0,31.0"})
    ids = [feature["properties"]["id"] for feature in response.json()["features"]]
    assert ids == ["withgeom"]


async def test_invalid_bbox_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(ROUTE, params={"bbox": "1,2,3"})
    assert response.status_code == 400
    assert "bbox" in response.json()["detail"]


async def test_point_and_bbox_together_returns_400(api_client: AsyncClient) -> None:
    response = await api_client.get(
        ROUTE, params={"point": "29.76,-95.37", "bbox": "-96,29,-94,31"}
    )
    assert response.status_code == 400
    assert "mutually exclusive" in response.json()["detail"]


async def test_bbox_combined_with_severity_filter(
    api_client: AsyncClient, integration_db: Database
) -> None:
    inside = _polygon(-95.7, 29.5, -95.0, 30.0)
    await _seed(
        integration_db,
        _alert("ext-inside", geometry=inside, severity=Severity.EXTREME),
        _alert("min-inside", geometry=inside, severity=Severity.MINOR),
        _alert(
            "ext-outside", geometry=_polygon(-100.0, 40.0, -99.0, 41.0), severity=Severity.EXTREME
        ),
    )
    response = await api_client.get(
        ROUTE,
        params={"bbox": "-96.0,29.0,-94.0,31.0", "severity": "Severe"},
    )
    ids = {feature["properties"]["id"] for feature in response.json()["features"]}
    assert ids == {"ext-inside"}


# --------------------------------------------------------------------------- #
# Detail endpoint — /v1/alerts/{id}                                            #
# --------------------------------------------------------------------------- #

DETAIL_ROUTE_TEMPLATE: str = "/v1/alerts/{alert_id}"


async def test_detail_returns_full_alert_including_description(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(
        integration_db,
        _alert(
            "with-prose",
            description="At 745 PM CDT, a severe thunderstorm was located...",
            instruction="Move to an interior room on the lowest floor.",
        ),
    )
    response = await api_client.get(DETAIL_ROUTE_TEMPLATE.format(alert_id="with-prose"))
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "Feature"
    props = body["properties"]
    assert props["id"] == "with-prose"
    assert props["description"].startswith("At 745 PM CDT")
    assert props["instruction"].startswith("Move to an interior room")
    # Camel-case aliases preserved at the wire layer.
    assert props["areaDesc"] == "Test Area"


async def test_detail_returns_404_for_unknown_id(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed(integration_db, _alert("present"))
    response = await api_client.get(DETAIL_ROUTE_TEMPLATE.format(alert_id="missing"))
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


async def test_detail_returns_alert_with_expired_timestamp(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """Detail shows alerts even after they expire — only the list endpoint
    filters by activeness."""
    await _seed(integration_db, _alert("expired", expires_offset=timedelta(hours=-1)))
    response = await api_client.get(DETAIL_ROUTE_TEMPLATE.format(alert_id="expired"))
    assert response.status_code == 200
    assert response.json()["properties"]["id"] == "expired"


async def test_detail_handles_urn_style_id_with_colons(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """NWS alert ids are URNs with colons; the route uses ``{id:path}`` so
    they survive routing intact."""
    urn_id = "urn:oid:2.49.0.1.840.0.test.001"
    await _seed(integration_db, _alert(urn_id))
    response = await api_client.get(DETAIL_ROUTE_TEMPLATE.format(alert_id=urn_id))
    assert response.status_code == 200
    assert response.json()["properties"]["id"] == urn_id
