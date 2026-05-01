"""Integration and unit tests for the SSE endpoint ``GET /v1/alerts/stream``.

Note on transport: httpx's ``ASGITransport`` fully buffers a streaming
response body until the ASGI app returns (it awaits ``app(scope, receive,
send)`` to completion before constructing the ``Response``), so an
infinite SSE stream would deadlock. Tests push N alerts, close the
subscriber to terminate the handler cleanly, then parse the buffered
response body. The unit-level generator tests at the bottom bypass HTTP
entirely and exercise the streaming path directly.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from aeroza.ingest.nws_alerts import Alert
from aeroza.main import create_app
from aeroza.query.v1 import _alert_event_stream
from aeroza.shared.db import Database
from aeroza.stream.subscriber import InMemoryAlertSubscriber

ROUTE: str = "/v1/alerts/stream"


def _alert(alert_id: str, event: str = "Severe Thunderstorm Warning") -> Alert:
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": event,
            "headline": f"{event} for {alert_id}",
            "severity": "Severe",
            "urgency": "Immediate",
            "certainty": "Observed",
            "sender_name": "NWS Test",
            "area_desc": "Test Area",
        }
    )


def _parse_sse(buffer: str) -> list[dict[str, str]]:
    """Split a raw SSE text buffer into a list of {event, id, data} dicts.

    Comment lines (starting with ``:``) are returned as ``{"comment": ...}``.
    """
    events: list[dict[str, str]] = []
    for raw in buffer.split("\n\n"):
        block = raw.strip("\n")
        if not block:
            continue
        if block.startswith(":"):
            events.append({"comment": block[1:].lstrip()})
            continue
        record: dict[str, str] = {}
        for line in block.split("\n"):
            field, _, value = line.partition(":")
            record[field.strip()] = value.lstrip()
        events.append(record)
    return events


# --------------------------------------------------------------------------- #
# Unit-level tests for the SSE generator (no HTTP transport).                  #
# --------------------------------------------------------------------------- #


@pytest.mark.unit
async def test_event_stream_emits_connected_then_alert_events() -> None:
    subscriber = InMemoryAlertSubscriber(initial=[_alert("a"), _alert("b")])
    chunks: list[bytes] = []

    async def consume() -> None:
        async for chunk in _alert_event_stream(subscriber):
            chunks.append(chunk)

    task = asyncio.create_task(consume())
    await subscriber.wait_for_subscriber_count(1, timeout=2.0)
    await subscriber.close()
    await asyncio.wait_for(task, timeout=2.0)

    body = b"".join(chunks).decode("utf-8")
    events = _parse_sse(body)
    assert events[0] == {"comment": "connected"}
    assert events[1]["event"] == "alert"
    assert events[1]["id"] == "a"
    payload_a = json.loads(events[1]["data"])
    assert payload_a["id"] == "a"
    assert payload_a["areaDesc"] == "Test Area"  # alias preserved on the wire
    assert events[2]["event"] == "alert"
    assert events[2]["id"] == "b"


@pytest.mark.unit
async def test_event_stream_terminates_when_subscriber_closes() -> None:
    subscriber = InMemoryAlertSubscriber()
    chunks: list[bytes] = []

    async def consume() -> None:
        async for chunk in _alert_event_stream(subscriber):
            chunks.append(chunk)

    task = asyncio.create_task(consume())
    await subscriber.wait_for_subscriber_count(1, timeout=2.0)
    await subscriber.close()
    await asyncio.wait_for(task, timeout=2.0)
    # Only the ``: connected`` comment should have been emitted.
    body = b"".join(chunks).decode("utf-8")
    events = _parse_sse(body)
    assert events == [{"comment": "connected"}]


@pytest.mark.unit
async def test_event_stream_propagates_pushed_alerts() -> None:
    subscriber = InMemoryAlertSubscriber()
    chunks: list[bytes] = []

    async def consume() -> None:
        async for chunk in _alert_event_stream(subscriber):
            chunks.append(chunk)

    task = asyncio.create_task(consume())
    await subscriber.wait_for_subscriber_count(1, timeout=2.0)
    await subscriber.push(_alert("late-1"))
    await subscriber.push(_alert("late-2"))
    await subscriber.close()
    await asyncio.wait_for(task, timeout=2.0)

    events = _parse_sse(b"".join(chunks).decode("utf-8"))
    ids = [e["id"] for e in events if e.get("event") == "alert"]
    assert ids == ["late-1", "late-2"]


# --------------------------------------------------------------------------- #
# HTTP-level integration tests.                                                #
# Each test pushes a fixed batch of alerts then closes the subscriber so the   #
# SSE handler exits cleanly and httpx's buffering ASGITransport returns.       #
# --------------------------------------------------------------------------- #


@pytest.mark.integration
async def test_http_returns_event_stream_with_seeded_alerts(
    api_client: AsyncClient,
    alert_subscriber: InMemoryAlertSubscriber,
) -> None:
    request_task = asyncio.create_task(api_client.get(ROUTE))
    await alert_subscriber.wait_for_subscriber_count(1, timeout=3.0)
    await alert_subscriber.push(_alert("a"))
    await alert_subscriber.push(_alert("b"))
    await alert_subscriber.close()
    response = await asyncio.wait_for(request_task, timeout=5.0)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(response.text)
    assert events[0] == {"comment": "connected"}
    alert_events = [e for e in events if e.get("event") == "alert"]
    assert [e["id"] for e in alert_events] == ["a", "b"]
    payload_b = json.loads(alert_events[1]["data"])
    assert payload_b["id"] == "b"
    assert payload_b["senderName"] == "NWS Test"  # camelCase alias preserved


@pytest.mark.integration
async def test_http_503_when_subscriber_state_missing(
    integration_db: Database,
) -> None:
    """A separate app instance with no subscriber on state must surface 503."""
    app = create_app()
    app.state.db = integration_db
    # Intentionally do NOT set app.state.subscriber.
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get(ROUTE)
    assert response.status_code == 503
    assert response.json()["detail"] == "streaming not available"


@pytest.mark.integration
async def test_http_no_cache_headers_on_stream_response(
    api_client: AsyncClient,
    alert_subscriber: InMemoryAlertSubscriber,
) -> None:
    request_task = asyncio.create_task(api_client.get(ROUTE))
    await alert_subscriber.wait_for_subscriber_count(1, timeout=3.0)
    await alert_subscriber.close()
    response = await asyncio.wait_for(request_task, timeout=5.0)

    assert response.status_code == 200
    assert response.headers.get("cache-control") == "no-cache"
    assert response.headers.get("x-accel-buffering") == "no"


@pytest.mark.integration
async def test_http_each_request_gets_its_own_subscription(
    api_client: AsyncClient,
    alert_subscriber: InMemoryAlertSubscriber,
) -> None:
    """Two concurrent SSE clients should each receive every published alert."""
    a_task = asyncio.create_task(api_client.get(ROUTE))
    b_task = asyncio.create_task(api_client.get(ROUTE))
    await alert_subscriber.wait_for_subscriber_count(2, timeout=3.0)
    await alert_subscriber.push(_alert("p"))
    await alert_subscriber.push(_alert("q"))
    await alert_subscriber.close()
    a_response, b_response = await asyncio.wait_for(asyncio.gather(a_task, b_task), timeout=5.0)

    a_ids = [e["id"] for e in _parse_sse(a_response.text) if e.get("event") == "alert"]
    b_ids = [e["id"] for e in _parse_sse(b_response.text) if e.get("event") == "alert"]
    assert a_ids == ["p", "q"]
    assert b_ids == ["p", "q"]
