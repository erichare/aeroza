"""Unit tests for the NWS active-alerts ingest function."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from aeroza.ingest.nws_alerts import (
    ACTIVE_ALERTS_PATH,
    NWS_BASE_URL,
    Alert,
    Certainty,
    NwsAlertsError,
    Severity,
    Urgency,
    fetch_active_alerts,
)
from aeroza.shared.http import http_client


def _alert_feature(**overrides: Any) -> dict[str, Any]:
    base = {
        "id": "urn:oid:2.49.0.1.840.0.test.001",
        "areaDesc": "Harris County, TX",
        "senderName": "NWS Houston/Galveston TX",
        "event": "Severe Thunderstorm Warning",
        "headline": "Severe Thunderstorm Warning until 8:30 PM CDT",
        "description": "At 745 PM CDT, a severe thunderstorm was located...",
        "instruction": "For your protection move to an interior room.",
        "severity": "Severe",
        "certainty": "Observed",
        "urgency": "Immediate",
        "effective": "2026-05-01T00:45:00+00:00",
        "onset": "2026-05-01T00:45:00+00:00",
        "expires": "2026-05-01T01:30:00+00:00",
        "ends": "2026-05-01T01:30:00+00:00",
    }
    base.update(overrides)
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-95.7, 29.5], [-95.7, 30.0], [-95.0, 30.0], [-95.0, 29.5], [-95.7, 29.5]]
            ],
        },
        "properties": base,
    }


def _feature_collection(*features: dict[str, Any]) -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": list(features)}


@pytest.mark.unit
class TestFetchActiveAlerts:
    @respx.mock
    async def test_returns_parsed_alerts_from_full_url(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json=_feature_collection(_alert_feature())
        )
        alerts = await fetch_active_alerts()
        assert len(alerts) == 1
        alert = alerts[0]
        assert isinstance(alert, Alert)
        assert alert.event == "Severe Thunderstorm Warning"
        assert alert.severity is Severity.SEVERE
        assert alert.urgency is Urgency.IMMEDIATE
        assert alert.certainty is Certainty.OBSERVED
        assert alert.area_desc == "Harris County, TX"
        assert alert.expires == datetime(2026, 5, 1, 1, 30, tzinfo=UTC)
        assert alert.geometry is not None
        assert alert.geometry["type"] == "Polygon"

    @respx.mock
    async def test_passes_area_filter(self) -> None:
        route = respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json=_feature_collection()
        )
        alerts = await fetch_active_alerts(area="TX")
        assert alerts == ()
        assert route.calls.last is not None
        assert route.calls.last.request.url.params["area"] == "TX"

    @respx.mock
    async def test_passes_point_filter(self) -> None:
        route = respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json=_feature_collection()
        )
        await fetch_active_alerts(point=(29.76, -95.37))
        assert route.calls.last is not None
        assert route.calls.last.request.url.params["point"] == "29.76,-95.37"

    @respx.mock
    async def test_uses_injected_client_with_base_url(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json=_feature_collection(_alert_feature())
        )
        async with http_client(base_url=NWS_BASE_URL) as client:
            alerts = await fetch_active_alerts(client=client)
        assert len(alerts) == 1

    @respx.mock
    async def test_returns_empty_tuple_for_empty_collection(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(200, json=_feature_collection())
        assert await fetch_active_alerts() == ()

    @respx.mock
    async def test_skips_features_with_invalid_properties(self) -> None:
        good = _alert_feature()
        bad = _alert_feature(id=None)  # id is required
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json=_feature_collection(good, bad)
        )
        alerts = await fetch_active_alerts()
        assert len(alerts) == 1
        assert alerts[0].id == good["properties"]["id"]

    @respx.mock
    async def test_raises_on_http_error(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(503)
        with pytest.raises(NwsAlertsError, match="failed"):
            await fetch_active_alerts()

    @respx.mock
    async def test_raises_on_unexpected_payload_shape(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").respond(
            200, json={"type": "NotAFeatureCollection", "features": []}
        )
        with pytest.raises(NwsAlertsError, match="unexpected NWS payload"):
            await fetch_active_alerts()

    @respx.mock
    async def test_raises_on_network_failure(self) -> None:
        respx.get(f"{NWS_BASE_URL}{ACTIVE_ALERTS_PATH}").mock(
            side_effect=httpx.ConnectError("dns failure")
        )
        with pytest.raises(NwsAlertsError):
            await fetch_active_alerts()
