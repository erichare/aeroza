"""Unit tests for push dispatch logic (device selection is integration-tested)."""

from __future__ import annotations

from typing import Any

from aeroza.ingest.nws_alerts import Alert, Severity
from aeroza.push.apns import ApnsResult
from aeroza.push.dispatch import build_payload, dispatch_to_devices, should_dispatch
from aeroza.push.models import DeviceTokenRow


def _device(
    token: str, *, lat: float = 35.47, lng: float = -97.51, env: str = "production"
) -> DeviceTokenRow:
    return DeviceTokenRow(
        token=token, platform="ios", environment=env, location_lat=lat, location_lng=lng
    )


def _alert(*, severity: Severity = Severity.SEVERE) -> Alert:
    return Alert(
        id="urn:test:1", event="Tornado Warning", headline="Take cover now", severity=severity
    )


class _FakeSender:
    def __init__(self, results: dict[str, ApnsResult]) -> None:
        self._results = results
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def send(
        self, *, device_token: str, environment: str, payload: dict[str, Any]
    ) -> ApnsResult:
        self.calls.append((device_token, environment, payload))
        return self._results.get(device_token, ApnsResult(status_code=200))


def test_should_dispatch_only_severe_and_above() -> None:
    assert should_dispatch(_alert(severity=Severity.EXTREME))
    assert should_dispatch(_alert(severity=Severity.SEVERE))
    assert not should_dispatch(_alert(severity=Severity.MODERATE))
    assert not should_dispatch(_alert(severity=Severity.MINOR))


def test_build_payload_matches_extension_contract() -> None:
    payload = build_payload(_alert(), _device("tok"), base_url="https://api.aeroza.app")
    assert payload["aps"]["alert"]["title"] == "Tornado Warning"
    assert payload["aps"]["mutable-content"] == 1
    assert payload["alert_id"] == "urn:test:1"
    assert payload["aeroza_base_url"] == "https://api.aeroza.app"
    assert payload["lat"] == 35.47
    assert payload["lng"] == -97.51


async def test_dispatch_counts_sent_and_collects_unregistered() -> None:
    sender = _FakeSender(
        {
            "good": ApnsResult(status_code=200),
            "dead": ApnsResult(status_code=410, reason="Unregistered"),
        }
    )
    devices = [_device("good"), _device("dead"), _device("good2")]
    outcome = await dispatch_to_devices(
        sender=sender, devices=devices, alert=_alert(), base_url="https://api.aeroza.app"
    )
    assert outcome.sent == 2
    assert outcome.unregistered_tokens == ("dead",)
    assert len(sender.calls) == 3
