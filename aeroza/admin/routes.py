"""HTTP routes for the admin/seed-event flow.

These endpoints sit under ``/v1/admin/...`` and are intentionally
gated by the ``AEROZA_DEV_ADMIN_ENABLED`` env flag rather than the
versioned auth surface — they're admin-grade operations meant for
the local dev console (e.g. /demo's "Seed this event" button), not
public traffic. When the flag is false (or anything other than the
truthy values below) the routes return 404 so they're invisible to
discovery scans on a deployed install.

The flag is read at *request time*, not cached on ``Settings``,
matching :mod:`aeroza.auth.dependencies`'s pattern. Tests flip the
env between requests; production wires it once via the systemd unit
or the docker compose file.

Endpoints:

* ``POST /v1/admin/seed-event`` — kick off (or rejoin) a background
  seed for the given ``[since, until]`` window. Returns 202 with a
  snapshot of the task; idempotent under double-clicks.
* ``GET /v1/admin/seed-event/status`` — read-only snapshot. Same
  ``[since, until]`` query identifies which task; 404 if no task
  exists for that window.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Annotated, Final, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.admin.seed_event import (
    DEFAULT_LEVEL,
    DEFAULT_PRODUCT,
    SeedTask,
    SeedWindow,
    get_runner,
)
from aeroza.query.dependencies import get_session
from aeroza.shared.db import Database

# Env flag controlling whether the admin routes accept requests at all.
# Default ``true`` because the dev console is the primary consumer and
# this is a localhost-shaped feature; flip to ``false`` for any deploy
# that exposes the API to untrusted networks.
ADMIN_ENABLED_ENV_FLAG: Final[str] = "AEROZA_DEV_ADMIN_ENABLED"

# Maximum window length the seed pipeline accepts. The underlying
# lister caps at two UTC days; curated events all fit in <24h. Reject
# obviously-wrong windows up front so the runner doesn't spin on
# them.
MAX_WINDOW_HOURS: Final[int] = 36

router = APIRouter(prefix="/v1/admin", tags=["admin"])


def _admin_enabled() -> bool:
    """Truthiness check on the env flag. Read every call so tests
    can monkeypatch the env without rebuilding the FastAPI app.
    """
    raw = os.environ.get(ADMIN_ENABLED_ENV_FLAG, "true").lower()
    return raw in ("1", "true", "yes")


def require_admin_enabled() -> None:
    """Raise 404 when admin routes are gated off.

    404 (not 403) is deliberate — when the flag's off the routes
    should be invisible to discovery, the same posture nginx-style
    feature flags take.
    """
    if not _admin_enabled():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="not found",
        )


def _validate_window(*, since: datetime, until: datetime) -> SeedWindow:
    """Apply the same invariants the runner expects, with HTTP-shaped errors."""
    if since.tzinfo is None or until.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="since/until must be tz-aware ISO-8601 (e.g. '...Z' or '+00:00')",
        )
    if until <= since:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"until ({until.isoformat()}) must be after since ({since.isoformat()})",
        )
    if (until - since) > timedelta(hours=MAX_WINDOW_HOURS):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"window exceeds the {MAX_WINDOW_HOURS}h cap (curated events all fit)",
        )
    # Use the supplied product/level when given; the body model
    # supplies sensible defaults via the Pydantic shape so the
    # request body can be just ``{since, until}`` for the common path.
    return SeedWindow(
        since=since,
        until=until,
        product=DEFAULT_PRODUCT,
        level=DEFAULT_LEVEL,
    )


# --------------------------------------------------------------------------- #
# Wire shapes                                                                 #
# --------------------------------------------------------------------------- #


class SeedEventRequest(BaseModel):
    """Body for ``POST /v1/admin/seed-event``."""

    model_config = ConfigDict(populate_by_name=True)

    since: AwareDatetime = Field(
        description="Inclusive lower bound of the historical window (ISO-8601, tz-aware).",
        examples=["2021-02-14T22:00:00Z"],
    )
    until: AwareDatetime = Field(
        description="Exclusive upper bound of the historical window (ISO-8601, tz-aware).",
        examples=["2021-02-15T16:00:00Z"],
    )
    product: str = Field(
        default=DEFAULT_PRODUCT,
        description="MRMS product (defaults to MergedReflectivityComposite).",
    )
    level: str = Field(
        default=DEFAULT_LEVEL,
        description="MRMS product level (defaults to '00.50').",
    )


class SeedEventTaskSnapshot(BaseModel):
    """Wire shape echoed by both endpoints.

    Mirrors :class:`aeroza.admin.seed_event.SeedTask` — every field
    that's safe to expose, plus a derived ``state`` so callers don't
    have to compute it from ``finished_at`` / ``error``.
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    type: Literal["AdminSeedEventTask"] = "AdminSeedEventTask"
    since: datetime
    until: datetime
    product: str
    level: str
    started_at: datetime = Field(serialization_alias="startedAt")
    finished_at: datetime | None = Field(default=None, serialization_alias="finishedAt")
    cfgrib_available: bool = Field(serialization_alias="cfgribAvailable")
    files_inserted: int = Field(serialization_alias="filesInserted")
    files_updated: int = Field(serialization_alias="filesUpdated")
    grids_materialised: int = Field(serialization_alias="gridsMaterialised")
    error: str | None = None
    state: Literal["running", "succeeded", "failed"]


def _to_snapshot(task: SeedTask) -> SeedEventTaskSnapshot:
    if task.finished_at is None:
        state: Literal["running", "succeeded", "failed"] = "running"
    elif task.error is not None:
        state = "failed"
    else:
        state = "succeeded"
    return SeedEventTaskSnapshot(
        since=task.window.since,
        until=task.window.until,
        product=task.window.product,
        level=task.window.level,
        started_at=task.started_at,
        finished_at=task.finished_at,
        cfgrib_available=task.cfgrib_available,
        files_inserted=task.files_inserted,
        files_updated=task.files_updated,
        grids_materialised=task.grids_materialised,
        error=task.error,
        state=state,
    )


# --------------------------------------------------------------------------- #
# Routes                                                                      #
# --------------------------------------------------------------------------- #


@router.post(
    "/seed-event",
    response_model=SeedEventTaskSnapshot,
    response_model_by_alias=True,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Seed a curated historical event into the local archive",
    description=(
        "Background-fires the ingest → materialise pipeline for the "
        "given ``[since, until]`` window. Returns immediately with a "
        "task snapshot; poll ``GET /v1/admin/seed-event/status`` for "
        "progress. Idempotent: a second POST for the same window "
        "returns the existing in-flight task.\n\n"
        "Gated by the ``AEROZA_DEV_ADMIN_ENABLED`` env flag (default "
        "true). When the flag is false, the route 404s."
    ),
    dependencies=[Depends(require_admin_enabled)],
)
async def post_seed_event_route(
    request: Request,
    body: SeedEventRequest,
    _session: Annotated[AsyncSession, Depends(get_session)],
) -> SeedEventTaskSnapshot:
    # Use the shared Database from app.state — same lifetime as the
    # rest of the API, and (importantly) the same connection pool the
    # background task hands out per-session.
    db: Database = request.app.state.db
    window = SeedWindow(
        since=body.since,
        until=body.until,
        product=body.product,
        level=body.level,
    )
    # Re-validate via the HTTP-shaped helper so a too-wide window
    # surfaces as 400, not a generic 500 from inside the runner.
    _validate_window(since=window.since, until=window.until)
    task = await get_runner().start(db=db, window=window)
    return _to_snapshot(task)


@router.get(
    "/seed-event/status",
    response_model=SeedEventTaskSnapshot,
    response_model_by_alias=True,
    summary="Read-only snapshot of a seed-event task",
    description=(
        "Returns the current state of the seed for the given "
        "``[since, until]`` window, or 404 if no task exists. The "
        "client polls this every few seconds while the button shows "
        "a progress spinner; once ``state`` flips to ``succeeded`` "
        "or ``failed`` the UI stops polling."
    ),
    responses={404: {"description": "No task for the requested window."}},
    dependencies=[Depends(require_admin_enabled)],
)
async def get_seed_event_status_route(
    since: Annotated[
        AwareDatetime,
        Query(description="Inclusive lower bound (ISO-8601, tz-aware)."),
    ],
    until: Annotated[
        AwareDatetime,
        Query(description="Exclusive upper bound (ISO-8601, tz-aware)."),
    ],
    product: Annotated[
        str,
        Query(description="MRMS product."),
    ] = DEFAULT_PRODUCT,
    level: Annotated[
        str,
        Query(description="MRMS product level."),
    ] = DEFAULT_LEVEL,
) -> SeedEventTaskSnapshot:
    window = SeedWindow(since=since, until=until, product=product, level=level)
    task = get_runner().status(window)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no seed task for that window",
        )
    return _to_snapshot(task)


__all__ = [
    "ADMIN_ENABLED_ENV_FLAG",
    "MAX_WINDOW_HOURS",
    "SeedEventRequest",
    "SeedEventTaskSnapshot",
    "router",
]
