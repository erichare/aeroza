"""Unit tests for the push device schemas (no DB, no APNs)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from aeroza.push.schemas import DeviceRegistration, DeviceResponse


def test_registration_accepts_valid_payload() -> None:
    reg = DeviceRegistration(
        token="a1b2c3d4e5f6",
        platform="ios",
        latitude=35.47,
        longitude=-97.51,
    )
    assert reg.environment == "production"  # default
    assert reg.latitude == 35.47


@pytest.mark.parametrize("lat", [-90.1, 90.1])
def test_registration_rejects_out_of_range_latitude(lat: float) -> None:
    with pytest.raises(ValidationError):
        DeviceRegistration(token="a1b2c3d4", platform="ios", latitude=lat, longitude=0.0)


@pytest.mark.parametrize("lng", [-180.1, 180.1])
def test_registration_rejects_out_of_range_longitude(lng: float) -> None:
    with pytest.raises(ValidationError):
        DeviceRegistration(token="a1b2c3d4", platform="ios", latitude=0.0, longitude=lng)


def test_registration_rejects_unknown_platform() -> None:
    with pytest.raises(ValidationError):
        DeviceRegistration(token="a1b2c3d4", platform="android")  # type: ignore[arg-type]


def test_registration_rejects_short_token() -> None:
    with pytest.raises(ValidationError):
        DeviceRegistration(token="abc", platform="ios")


def test_response_serialises_camelcase_aliases() -> None:
    resp = DeviceResponse(
        token="a1b2c3d4",
        platform="ios",
        environment="production",
        registered_at=datetime(2026, 6, 3, tzinfo=UTC),
        updated_at=datetime(2026, 6, 3, tzinfo=UTC),
    )
    dumped = resp.model_dump(by_alias=True)
    assert "registeredAt" in dumped
    assert "updatedAt" in dumped
