"""Route tests for the admin seed-event API.

The seed pipeline itself is exercised in
:mod:`tests.test_admin_seed_event_runner` (no S3 round-trip — those
tests stub the runner's ``start`` to assert the wiring). Here we
focus on the *HTTP shape*: env-flag gating, body/query validation,
404 when no task exists, and the snapshot envelope's wire fields.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient

from aeroza.admin.routes import ADMIN_ENABLED_ENV_FLAG
from aeroza.admin.seed_event import (
    DEFAULT_LEVEL,
    DEFAULT_PRODUCT,
    SeedEventRunner,
    set_runner,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _reset_runner() -> None:
    """Each test gets a fresh runner; otherwise tasks from previous
    cases bleed via the module-level singleton."""
    set_runner(SeedEventRunner())


@pytest.fixture(autouse=True)
def _enable_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default to admin enabled for the test session — individual
    tests opt out via ``monkeypatch.setenv(...)``."""
    monkeypatch.setenv(ADMIN_ENABLED_ENV_FLAG, "true")


def _window_payload(
    *,
    since: datetime,
    until: datetime,
    product: str = DEFAULT_PRODUCT,
    level: str = DEFAULT_LEVEL,
) -> dict[str, str]:
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "product": product,
        "level": level,
    }


# --------------------------------------------------------------------------- #
# Env-flag gating                                                             #
# --------------------------------------------------------------------------- #


async def test_routes_404_when_admin_disabled(
    api_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabling the flag makes both routes invisible (404, not 403) so
    discovery scans on a deployed install can't tell the difference
    between 'feature off' and 'feature absent'."""
    monkeypatch.setenv(ADMIN_ENABLED_ENV_FLAG, "false")

    since = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    until = since + timedelta(hours=1)

    post = await api_client.post(
        "/v1/admin/seed-event",
        json=_window_payload(since=since, until=until),
    )
    assert post.status_code == 404

    status = await api_client.get(
        "/v1/admin/seed-event/status",
        params={"since": since.isoformat(), "until": until.isoformat()},
    )
    assert status.status_code == 404


# --------------------------------------------------------------------------- #
# Body validation                                                             #
# --------------------------------------------------------------------------- #


async def test_post_rejects_naive_timestamps(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/v1/admin/seed-event",
        json={
            "since": "2026-05-01T12:00:00",  # naive
            "until": "2026-05-01T13:00:00",
        },
    )
    # Pydantic's AwareDatetime rejects naive inputs with 422.
    assert response.status_code == 422


async def test_post_rejects_inverted_window(api_client: AsyncClient) -> None:
    until = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    since = until + timedelta(hours=1)
    response = await api_client.post(
        "/v1/admin/seed-event",
        json=_window_payload(since=since, until=until),
    )
    assert response.status_code == 400
    assert "must be after" in response.json()["detail"]


async def test_post_rejects_window_wider_than_cap(api_client: AsyncClient) -> None:
    since = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    until = since + timedelta(hours=72)
    response = await api_client.post(
        "/v1/admin/seed-event",
        json=_window_payload(since=since, until=until),
    )
    assert response.status_code == 400
    assert "cap" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# Status route                                                                #
# --------------------------------------------------------------------------- #


async def test_status_404_when_no_task_for_window(api_client: AsyncClient) -> None:
    since = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    until = since + timedelta(hours=1)
    response = await api_client.get(
        "/v1/admin/seed-event/status",
        params={"since": since.isoformat(), "until": until.isoformat()},
    )
    assert response.status_code == 404
    assert "no seed task" in response.json()["detail"]


async def test_post_returns_202_with_running_snapshot(
    api_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: POST kicks off the runner and returns a 202 with
    the task snapshot. We monkeypatch ``_run`` to a no-op coroutine
    so the test doesn't try to talk to S3."""
    import asyncio

    async def _no_op_run(*args: object, **kwargs: object) -> None:
        # Sleep briefly so the task is observably "running" when the
        # route returns — without this the asyncio.create_task loop
        # could hand control back instantly and the snapshot would
        # already be in the ``succeeded`` state.
        await asyncio.sleep(0.01)

    monkeypatch.setattr(SeedEventRunner, "_run", _no_op_run)

    since = datetime(2021, 2, 14, 22, 0, tzinfo=UTC)
    until = datetime(2021, 2, 15, 16, 0, tzinfo=UTC)

    post = await api_client.post(
        "/v1/admin/seed-event",
        json=_window_payload(since=since, until=until),
    )
    assert post.status_code == 202
    body = post.json()
    assert body["type"] == "AdminSeedEventTask"
    assert body["since"] == since.isoformat().replace("+00:00", "Z") or body["since"].startswith(
        since.isoformat()[:19]
    )
    assert body["state"] in {"running", "succeeded"}
    assert body["product"] == DEFAULT_PRODUCT
    assert body["level"] == DEFAULT_LEVEL


async def test_status_returns_existing_task_after_post(
    api_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a POST, the same window's status endpoint returns the
    in-flight task instead of 404. Idempotency on the runner side
    makes this safe under double-clicks."""
    import asyncio

    async def _no_op_run(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(0.05)

    monkeypatch.setattr(SeedEventRunner, "_run", _no_op_run)

    since = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    until = since + timedelta(hours=2)

    posted = await api_client.post(
        "/v1/admin/seed-event",
        json=_window_payload(since=since, until=until),
    )
    assert posted.status_code == 202

    status = await api_client.get(
        "/v1/admin/seed-event/status",
        params={"since": since.isoformat(), "until": until.isoformat()},
    )
    assert status.status_code == 200
    assert status.json()["type"] == "AdminSeedEventTask"
