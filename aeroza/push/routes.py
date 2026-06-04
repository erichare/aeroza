"""``/v1/push/*`` routes — device registration for severe-weather alerts.

Registration is anonymous-friendly: an install POSTs its APNs token and saved
location with no API key. When a BYO-key user is authenticated, the row is
associated with their key so we can honour per-key policies later.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.auth.dependencies import AuthenticatedKey, get_optional_api_key
from aeroza.push import store
from aeroza.push.models import DeviceTokenRow
from aeroza.push.schemas import DeviceRegistration, DeviceResponse
from aeroza.query.dependencies import get_session

router = APIRouter(prefix="/push", tags=["push"])


def _to_response(row: DeviceTokenRow) -> DeviceResponse:
    return DeviceResponse(
        token=row.token,
        platform=row.platform,
        environment=row.environment,
        latitude=row.location_lat,
        longitude=row.location_lng,
        registered_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.post(
    "/devices",
    response_model=DeviceResponse,
    response_model_by_alias=True,
    status_code=status.HTTP_201_CREATED,
    summary="Register (or update) a device for severe-weather push",
)
async def register_device(
    registration: DeviceRegistration,
    session: Annotated[AsyncSession, Depends(get_session)],
    key: Annotated[AuthenticatedKey | None, Depends(get_optional_api_key)] = None,
) -> DeviceResponse:
    api_key_id = uuid.UUID(key.id) if key is not None else None
    row = await store.upsert_device(session, registration, api_key_id=api_key_id)
    return _to_response(row)


@router.get(
    "/devices/{token}",
    response_model=DeviceResponse,
    response_model_by_alias=True,
    summary="Look up a registered device by token",
)
async def get_device_route(
    token: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DeviceResponse:
    row = await store.get_device(session, token)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="device not registered")
    return _to_response(row)


@router.delete(
    "/devices/{token}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deregister a device (opt out of push)",
)
async def delete_device_route(
    token: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    deleted = await store.delete_device(session, token)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="device not registered")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
