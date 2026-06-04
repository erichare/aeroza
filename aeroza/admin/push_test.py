"""Admin-only test-push endpoint.

Sends a one-off APNs push to an already-registered device so an operator can
verify delivery end-to-end without waiting for real severe weather. Gated by
the same ``AEROZA_DEV_ADMIN_ENABLED`` flag as the rest of ``/v1/admin`` (404
when off — set it false before a public launch). It only targets a token that
is already in ``device_tokens``, so it can't be used to push to arbitrary
devices, and it returns the raw APNs response (status + reason) for debugging.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.admin.routes import require_admin_enabled
from aeroza.config import get_settings
from aeroza.ingest.nws_alerts import Alert
from aeroza.push import store
from aeroza.push.dispatch import build_apns_sender, build_payload
from aeroza.query.dependencies import get_session

router = APIRouter(
    prefix="/v1/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin_enabled)],
)


class TestPushRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    token: str = Field(min_length=8, description="A registered APNs device token.")
    title: str = "Aeroza One"
    body: str = "Test push — severe-weather alerts are wired up."


class TestPushResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ok: bool
    status_code: int = Field(serialization_alias="statusCode")
    apns_id: str | None = Field(default=None, serialization_alias="apnsId")
    reason: str | None = None


@router.post(
    "/push/test",
    response_model=TestPushResult,
    response_model_by_alias=True,
    summary="Send a test APNs push to a registered device",
)
async def send_test_push(
    payload: TestPushRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TestPushResult:
    device = await store.get_device(session, payload.token)
    if device is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="device not registered")

    settings = get_settings()
    sender = build_apns_sender(settings)
    if sender is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="APNs not configured — set AEROZA_APNS_KEY_ID / _TEAM_ID / _PRIVATE_KEY",
        )

    # Reuse the real dispatch payload so the push behaves exactly like a warning
    # (mutable-content, lat/lng → the Notification Service Extension hydrates it).
    alert = Alert(id="aeroza-test-push", event=payload.title, headline=payload.body)
    body = build_payload(alert, device, base_url=settings.public_api_base_url)
    try:
        result = await sender.send(
            device_token=device.token, environment=device.environment, payload=body
        )
    finally:
        await sender.aclose()

    return TestPushResult(
        ok=result.ok,
        status_code=result.status_code,
        apns_id=result.apns_id,
        reason=result.reason,
    )
