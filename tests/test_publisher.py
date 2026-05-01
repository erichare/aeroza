"""Unit tests for AlertPublisher implementations."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from aeroza.ingest.nws_alerts import Alert
from aeroza.stream.publisher import (
    AlertPublisher,
    InMemoryAlertPublisher,
    NullAlertPublisher,
)


def _alert(alert_id: str) -> Alert:
    return Alert.model_validate(
        {
            "id": alert_id,
            "event": "Severe Thunderstorm Warning",
        }
    )


@pytest.mark.unit
class TestInMemoryAlertPublisher:
    async def test_captures_published_alerts_in_order(self) -> None:
        publisher = InMemoryAlertPublisher()
        await publisher.publish_new_alert(_alert("a"))
        await publisher.publish_new_alert(_alert("b"))
        await publisher.publish_new_alert(_alert("c"))
        assert publisher.published_ids == ("a", "b", "c")
        assert all(isinstance(a, Alert) for a in publisher.published)

    async def test_published_property_is_immutable_snapshot(self) -> None:
        publisher = InMemoryAlertPublisher()
        await publisher.publish_new_alert(_alert("a"))
        snapshot = publisher.published
        await publisher.publish_new_alert(_alert("b"))
        # The original snapshot must not have grown — `published` returns a tuple
        # built fresh each call, never a live view.
        assert snapshot == (publisher.published[0],)

    async def test_clear_drops_history(self) -> None:
        publisher = InMemoryAlertPublisher()
        await publisher.publish_new_alert(_alert("a"))
        publisher.clear()
        assert publisher.published == ()
        assert publisher.published_ids == ()

    def test_satisfies_alert_publisher_protocol(self) -> None:
        # Static check via runtime isinstance is not possible without
        # @runtime_checkable; assert structurally instead.
        publisher: AlertPublisher = InMemoryAlertPublisher()
        assert hasattr(publisher, "publish_new_alert")


@pytest.mark.unit
class TestNullAlertPublisher:
    async def test_drops_silently(self) -> None:
        publisher = NullAlertPublisher()
        # Should not raise, should not record anywhere observable.
        await publisher.publish_new_alert(_alert("a"))
        await publisher.publish_new_alert(_alert("b"))
        # Nothing else to assert — the contract is "no observable side effect".
        assert isinstance(publisher, NullAlertPublisher)


@pytest.mark.unit
def test_protocol_does_not_leak_basemodel_helpers() -> None:
    """Sanity that AlertPublisher is a Protocol, not a BaseModel — it must
    accept arbitrary classes that satisfy ``publish_new_alert`` without
    inheriting from anything."""

    class _Bespoke:
        async def publish_new_alert(self, alert: Alert) -> None:
            pass

    publisher: AlertPublisher = _Bespoke()
    assert publisher is not None
    # And the in-memory publisher is *not* a pydantic model.
    assert not isinstance(InMemoryAlertPublisher(), BaseModel)
