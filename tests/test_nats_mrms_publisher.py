"""Unit tests for :class:`NatsMrmsFilePublisher`.

Stubs the NATS client so we can verify subject and payload encoding
without a live broker.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from aeroza.ingest.mrms import MrmsFile
from aeroza.stream.nats import (
    MRMS_NEW_FILE_SUBJECT,
    NatsMrmsFilePublisher,
)

pytestmark = pytest.mark.unit


class StubNatsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append((subject, payload))


def _file(
    *,
    key: str = (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
    ),
    valid_at: datetime | None = None,
    etag: str | None = "deadbeef",
) -> MrmsFile:
    return MrmsFile(
        key=key,
        product="MergedReflectivityComposite",
        level="00.50",
        valid_at=valid_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        size_bytes=10_000,
        etag=etag,
    )


async def test_publishes_to_default_subject() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsFilePublisher(client)
    await publisher.publish_new_file(_file())
    assert len(client.calls) == 1
    subject, _payload = client.calls[0]
    assert subject == MRMS_NEW_FILE_SUBJECT
    assert publisher.subject == MRMS_NEW_FILE_SUBJECT


async def test_payload_is_json_with_camelcase_aliases() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsFilePublisher(client)
    file = _file()
    await publisher.publish_new_file(file)

    _subject, payload = client.calls[0]
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["key"] == file.key
    assert parsed["product"] == file.product
    assert parsed["level"] == file.level
    assert parsed["validAt"] == "2026-05-01T12:00:00+00:00"
    assert parsed["sizeBytes"] == file.size_bytes
    assert parsed["etag"] == "deadbeef"
    # Underscored names must NOT leak through to the wire.
    assert "valid_at" not in parsed
    assert "size_bytes" not in parsed


async def test_null_etag_round_trips() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsFilePublisher(client)
    await publisher.publish_new_file(_file(etag=None))
    parsed = json.loads(client.calls[0][1].decode("utf-8"))
    assert parsed["etag"] is None


async def test_overrides_subject() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsFilePublisher(client, subject="my.subject")
    await publisher.publish_new_file(_file())
    assert client.calls[0][0] == "my.subject"


async def test_propagates_client_errors() -> None:
    class BrokenClient:
        async def publish(self, subject: str, payload: bytes) -> None:
            raise RuntimeError(f"transport down for {subject}")

    publisher = NatsMrmsFilePublisher(BrokenClient())
    with pytest.raises(RuntimeError, match="transport down"):
        await publisher.publish_new_file(_file())


async def test_each_call_emits_one_message() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsFilePublisher(client)
    keys = ("a", "b", "c")
    for key in keys:
        await publisher.publish_new_file(_file(key=key))
    parsed = [json.loads(payload.decode())["key"] for _, payload in client.calls]
    assert tuple(parsed) == keys
