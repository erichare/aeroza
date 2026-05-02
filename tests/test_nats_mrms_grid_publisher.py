"""Unit tests for :class:`NatsMrmsGridPublisher`.

Stubs the NATS client so we can verify subject and payload encoding
without a live broker. Mirrors test_nats_mrms_publisher.py.
"""

from __future__ import annotations

import json

import pytest

from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.stream.nats import (
    MRMS_NEW_GRID_SUBJECT,
    NatsMrmsGridPublisher,
)

pytestmark = pytest.mark.unit


class StubNatsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append((subject, payload))


def _locator(
    *,
    file_key: str = (
        "CONUS/MergedReflectivityComposite_00.50/20260501/"
        "MRMS_MergedReflectivityComposite_00.50_20260501-120000.grib2.gz"
    ),
    zarr_uri: str = "/var/data/mrms.zarr",
    variable: str = "reflectivity",
    dims: tuple[str, ...] = ("latitude", "longitude"),
    shape: tuple[int, ...] = (3500, 7000),
    dtype: str = "float32",
    nbytes: int = 3500 * 7000 * 4,
) -> MrmsGridLocator:
    return MrmsGridLocator(
        file_key=file_key,
        zarr_uri=zarr_uri,
        variable=variable,
        dims=dims,
        shape=shape,
        dtype=dtype,
        nbytes=nbytes,
    )


async def test_publishes_to_default_subject() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsGridPublisher(client)
    await publisher.publish_new_grid(_locator())
    assert len(client.calls) == 1
    subject, _payload = client.calls[0]
    assert subject == MRMS_NEW_GRID_SUBJECT
    assert publisher.subject == MRMS_NEW_GRID_SUBJECT


async def test_payload_is_json_with_camelcase_aliases() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsGridPublisher(client)
    locator = _locator()
    await publisher.publish_new_grid(locator)

    _subject, payload = client.calls[0]
    parsed = json.loads(payload.decode("utf-8"))
    assert parsed["fileKey"] == locator.file_key
    assert parsed["zarrUri"] == locator.zarr_uri
    assert parsed["variable"] == "reflectivity"
    assert parsed["dims"] == ["latitude", "longitude"]
    assert parsed["shape"] == [3500, 7000]
    assert parsed["dtype"] == "float32"
    assert parsed["nbytes"] == 3500 * 7000 * 4
    # Underscored names must NOT leak through to the wire.
    assert "file_key" not in parsed
    assert "zarr_uri" not in parsed


async def test_overrides_subject() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsGridPublisher(client, subject="my.subject")
    await publisher.publish_new_grid(_locator())
    assert client.calls[0][0] == "my.subject"


async def test_propagates_client_errors() -> None:
    class BrokenClient:
        async def publish(self, subject: str, payload: bytes) -> None:
            raise RuntimeError(f"transport down for {subject}")

    publisher = NatsMrmsGridPublisher(BrokenClient())
    with pytest.raises(RuntimeError, match="transport down"):
        await publisher.publish_new_grid(_locator())


async def test_each_call_emits_one_message() -> None:
    client = StubNatsClient()
    publisher = NatsMrmsGridPublisher(client)
    keys = ("a", "b", "c")
    for key in keys:
        await publisher.publish_new_grid(_locator(file_key=key))
    parsed = [json.loads(payload.decode())["fileKey"] for _, payload in client.calls]
    assert tuple(parsed) == keys
