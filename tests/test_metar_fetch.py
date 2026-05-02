"""Unit test for fetch_metar_observations using respx.

Confirms the fetcher passes the right query string, parses the
response correctly, and returns an empty tuple for an empty
``station_ids`` arg without making a request.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from aeroza.ingest.metar import (
    AWC_METAR_BASE_URL,
    AWC_METAR_PATH,
    fetch_metar_observations,
)

pytestmark = pytest.mark.unit


@respx.mock
async def test_fetch_passes_csv_station_ids_and_format() -> None:
    """Verifies the wire-shape contract with AWC: ids=KIAH,KHOU&format=json."""
    route = respx.get(f"{AWC_METAR_BASE_URL}{AWC_METAR_PATH}").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "icaoId": "KIAH",
                    "reportTime": "2026-05-02T18:00:00Z",
                    "lat": 29.98,
                    "lon": -95.34,
                    "rawOb": "KIAH 021800Z ...",
                }
            ],
        )
    )
    async with httpx.AsyncClient(base_url=AWC_METAR_BASE_URL) as client:
        observations = await fetch_metar_observations(
            station_ids=["KIAH", "KHOU"],
            client=client,
        )
    assert route.called
    assert "ids=KIAH%2CKHOU" in str(route.calls[0].request.url) or "ids=KIAH,KHOU" in str(
        route.calls[0].request.url
    )
    assert "format=json" in str(route.calls[0].request.url)
    assert len(observations) == 1
    assert observations[0].station_id == "KIAH"


async def test_fetch_with_empty_station_list_returns_empty_without_request() -> None:
    """Defensive — passing an empty list shouldn't even open a connection."""
    out = await fetch_metar_observations(station_ids=[])
    assert out == ()
