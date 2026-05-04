"""Unit tests for the historical-alerts proxy module.

The IEM endpoint itself is mocked — these tests exercise the
normalisation, deduping, batching, and route validation. The integration
test fleet exercises the live IEM round trip out-of-band; we do not want
test runs to depend on an external service being reachable.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from aeroza.query.historical_alerts import (
    HistoricalAlertQuery,
    _normalise_feature,
    fetch_historical_alerts,
    normalise_wfos,
    parse_wfo_list,
)

pytestmark = pytest.mark.unit


def _iem_feature(
    *,
    product_id: str,
    phenomena: str = "TO",
    event_label: str = "Tornado Warning",
    polygon_begin: str | None = "2024-05-16T22:03:00Z",
    polygon_end: str | None = "2024-05-16T22:30:00Z",
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-95.5, 29.8], [-95.4, 29.8], [-95.4, 29.9], [-95.5, 29.9], [-95.5, 29.8]]]
            ],
        },
        "properties": {
            "product_id": product_id,
            "phenomena": phenomena,
            "event_label": event_label,
            "wfo": "HGX",
            "utc_polygon_begin": polygon_begin,
            "utc_polygon_end": polygon_end,
            "utc_issue": polygon_begin,
            "utc_expire": polygon_end,
            "locations": "Harris [TX]",
        },
    }


class TestParseWfoList:
    def test_normal_list(self) -> None:
        assert parse_wfo_list("HGX,LCH,JAN") == ("HGX", "LCH", "JAN")

    def test_lowercase_uppercased(self) -> None:
        assert parse_wfo_list("hgx,lch") == ("HGX", "LCH")

    def test_whitespace_trimmed(self) -> None:
        assert parse_wfo_list(" HGX , LCH ") == ("HGX", "LCH")

    def test_empty_items_dropped(self) -> None:
        assert parse_wfo_list("HGX,,LCH,") == ("HGX", "LCH")

    def test_none(self) -> None:
        assert parse_wfo_list(None) == ()


class TestNormaliseWfos:
    def test_passthrough(self) -> None:
        assert normalise_wfos(["HGX", "LCH"]) == ("HGX", "LCH")

    def test_strips_and_uppercases(self) -> None:
        assert normalise_wfos([" hgx ", "lch"]) == ("HGX", "LCH")


class TestNormaliseFeature:
    def test_tornado_maps_to_extreme(self) -> None:
        feature = _normalise_feature(_iem_feature(product_id="t1", phenomena="TO"))
        assert feature is not None
        assert feature.properties.severity == "Extreme"
        assert feature.properties.event == "Tornado Warning"
        assert feature.properties.id == "t1"

    def test_severe_thunderstorm_maps_to_severe(self) -> None:
        feature = _normalise_feature(
            _iem_feature(
                product_id="s1",
                phenomena="SV",
                event_label="Severe Thunderstorm Warning",
            ),
        )
        assert feature is not None
        assert feature.properties.severity == "Severe"

    def test_unknown_phenomena_falls_back_to_moderate(self) -> None:
        feature = _normalise_feature(
            _iem_feature(product_id="x1", phenomena="ZZ", event_label="Mystery"),
        )
        assert feature is not None
        assert feature.properties.severity == "Moderate"

    def test_polygon_times_take_precedence(self) -> None:
        feature = _normalise_feature(
            _iem_feature(
                product_id="t2",
                polygon_begin="2024-05-16T22:05:00Z",
                polygon_end="2024-05-16T22:25:00Z",
            ),
        )
        assert feature is not None
        assert feature.properties.onset == datetime(2024, 5, 16, 22, 5, tzinfo=UTC)
        assert feature.properties.ends == datetime(2024, 5, 16, 22, 25, tzinfo=UTC)

    def test_missing_product_id_is_dropped(self) -> None:
        bad = _iem_feature(product_id="placeholder")
        bad["properties"].pop("product_id")
        assert _normalise_feature(bad) is None

    def test_non_dict_input_returns_none(self) -> None:
        assert _normalise_feature("not a dict") is None
        assert _normalise_feature(None) is None

    def test_sender_name_includes_wfo(self) -> None:
        feature = _normalise_feature(_iem_feature(product_id="t3"))
        assert feature is not None
        assert feature.properties.sender_name == "NWS HGX"


class TestFetchHistoricalAlertsBatching:
    @pytest.mark.asyncio
    async def test_chunks_more_than_three_wfos(self) -> None:
        # IEM caps at 3 WFOs per request — verify the backend splits a
        # 5-WFO query into two parallel calls and merges results.
        chunk_responses = [
            {"features": [_iem_feature(product_id="a1"), _iem_feature(product_id="a2")]},
            {"features": [_iem_feature(product_id="b1"), _iem_feature(product_id="b2")]},
        ]
        responses_iter = iter(chunk_responses)

        class FakeResponse:
            def __init__(self, payload: dict[str, Any]) -> None:
                self._payload = payload

            def raise_for_status(self) -> None:
                pass

            def json(self) -> Any:
                return self._payload

        async def fake_get(_self: Any, _url: str, **_kwargs: Any) -> FakeResponse:
            return FakeResponse(next(responses_iter))

        # Bypass cache by using a unique window per test run.
        query = HistoricalAlertQuery(
            since=datetime(2024, 5, 16, 22, 0, tzinfo=UTC),
            until=datetime(2024, 5, 17, 2, 30, tzinfo=UTC),
            wfos=("HGX", "LCH", "JAN", "MEG", "BMX"),
        )

        with patch("httpx.AsyncClient.get", new=fake_get):
            collection = await fetch_historical_alerts(query)

        ids = {f.properties.id for f in collection.features}
        assert ids == {"a1", "a2", "b1", "b2"}

    @pytest.mark.asyncio
    async def test_dedupes_across_chunks(self) -> None:
        # Same product id appearing in two chunks (a county-line warning
        # might be returned by both adjacent WFO queries) should land in
        # the merged collection only once.
        duplicate = _iem_feature(product_id="dup")
        chunk_responses = [
            {"features": [duplicate, _iem_feature(product_id="a1")]},
            {"features": [duplicate]},
        ]
        responses_iter = iter(chunk_responses)

        class FakeResponse:
            def __init__(self, payload: dict[str, Any]) -> None:
                self._payload = payload

            def raise_for_status(self) -> None:
                pass

            def json(self) -> Any:
                return self._payload

        async def fake_get(_self: Any, _url: str, **_kwargs: Any) -> FakeResponse:
            return FakeResponse(next(responses_iter))

        query = HistoricalAlertQuery(
            since=datetime(2024, 5, 16, 23, 0, tzinfo=UTC),
            until=datetime(2024, 5, 17, 3, 30, tzinfo=UTC),
            wfos=("HGX", "LCH", "JAN", "MEG"),
        )

        with patch("httpx.AsyncClient.get", new=fake_get):
            collection = await fetch_historical_alerts(query)

        ids = [f.properties.id for f in collection.features]
        assert ids.count("dup") == 1
        assert "a1" in ids
