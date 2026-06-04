"""Wire-format schemas for the push device endpoints.

camelCase on the wire (matching the rest of the v1 API and the Swift client's
Codable models); snake_case in Python via serialization aliases.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DevicePlatform = Literal["ios", "ipados", "macos", "watchos", "tvos"]
DeviceEnvironment = Literal["sandbox", "production"]


class DeviceRegistration(BaseModel):
    """Request body for ``POST /v1/push/devices``."""

    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    token: str = Field(min_length=8, max_length=512, description="Hex APNs device token.")
    platform: DevicePlatform
    environment: DeviceEnvironment = "production"
    latitude: float | None = Field(default=None, ge=-90.0, le=90.0)
    longitude: float | None = Field(default=None, ge=-180.0, le=180.0)


class DeviceResponse(BaseModel):
    """Response body describing a registered device."""

    model_config = ConfigDict(populate_by_name=True)

    token: str
    platform: str
    environment: str
    latitude: float | None = None
    longitude: float | None = None
    registered_at: datetime = Field(serialization_alias="registeredAt")
    updated_at: datetime = Field(serialization_alias="updatedAt")
