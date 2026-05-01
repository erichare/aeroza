"""Unit tests for :class:`NatsAlertPublisher`.

We don't run a real NATS in CI here — the publisher is a thin call wrapper
that delegates to ``client.publish``. Driving it with a stub client gives
us high-confidence coverage without adding a service container.
"""

from __future__ import annotations

import json

import pytest

from aeroza.ingest.nws_alerts import Alert
from aeroza.stream.nats import (
    NWS_NEW_ALERT_SUBJECT,
    NatsAlertPublisher,
    NatsPublisher,
)

pytestmark = pytest.mark.unit


class StubNatsClient:
    """In-memory NATS client that records every publish call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append((subject, payload))


def _alert(alert_id: str = "urn:test:alert:1", **overrides: object) -> Alert:
    base: dict[str, object] = {
        "id": alert_id,
        "event": "Severe Thunderstorm Warning",
        "headline": "Severe Thunderstorm Warning",
        "severity": "Severe",
        "urgency": "Immediate",
        "certainty": "Observed",
        "sender_name": "NWS Test",
        "area_desc": "Test Area",
    }
    base.update(overrides)
    return Alert.model_validate(base)


async def test_publishes_to_default_subject() -> None:
    client = StubNatsClient()
    publisher = NatsAlertPublisher(client)
    await publisher.publish_new_alert(_alert())
    assert len(client.calls) == 1
    subject, _ = client.calls[0]
    assert subject == NWS_NEW_ALERT_SUBJECT
    assert publisher.subject == NWS_NEW_ALERT_SUBJECT


async def test_payload_is_json_with_camelcase_aliases() -> None:
    client = StubNatsClient()
    publisher = NatsAlertPublisher(client)
    alert = _alert(alert_id="abc", area_desc="Harris County, TX")
    await publisher.publish_new_alert(alert)

    _, payload = client.calls[0]
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["id"] == "abc"
    # NWS-style camelCase aliases are preserved on the wire.
    assert parsed["areaDesc"] == "Harris County, TX"
    assert "area_desc" not in parsed
    assert parsed["senderName"] == "NWS Test"


async def test_overrides_subject() -> None:
    client = StubNatsClient()
    publisher = NatsAlertPublisher(client, subject="custom.subject")
    await publisher.publish_new_alert(_alert())
    subject, _ = client.calls[0]
    assert subject == "custom.subject"


async def test_propagates_client_errors() -> None:
    class BrokenClient:
        async def publish(self, subject: str, payload: bytes) -> None:
            raise RuntimeError(f"transport error on {subject}")

    publisher = NatsAlertPublisher(BrokenClient())
    with pytest.raises(RuntimeError, match="transport error"):
        await publisher.publish_new_alert(_alert())


async def test_each_call_emits_one_message() -> None:
    client = StubNatsClient()
    publisher = NatsAlertPublisher(client)
    await publisher.publish_new_alert(_alert("a"))
    await publisher.publish_new_alert(_alert("b"))
    await publisher.publish_new_alert(_alert("c"))
    assert [json.loads(p.decode())["id"] for _, p in client.calls] == ["a", "b", "c"]


def test_stub_satisfies_protocol() -> None:
    publisher: NatsPublisher = StubNatsClient()
    assert hasattr(publisher, "publish")
