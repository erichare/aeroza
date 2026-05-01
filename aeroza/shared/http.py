"""Shared async HTTP client factory for ingest and outbound integrations.

NWS and other public weather APIs require a contact-bearing User-Agent;
centralising the client here keeps that contract in one place and makes
mocking straightforward in tests (override via ``http_client(user_agent=...)``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from aeroza import __version__

DEFAULT_USER_AGENT: str = f"Aeroza/{__version__} (+https://github.com/erichare/aeroza)"
DEFAULT_TIMEOUT: httpx.Timeout = httpx.Timeout(10.0, connect=5.0)
DEFAULT_CONNECTION_RETRIES: int = 2


@asynccontextmanager
async def http_client(
    *,
    user_agent: str | None = None,
    timeout: httpx.Timeout | None = None,
    base_url: str | None = None,
) -> AsyncIterator[httpx.AsyncClient]:
    """Yield a configured ``httpx.AsyncClient`` and close it on exit.

    Connection-level retries (DNS, connect, read) are handled by the transport.
    Higher-level retry policies (5xx, rate-limit) are the caller's concern.
    """
    transport = httpx.AsyncHTTPTransport(retries=DEFAULT_CONNECTION_RETRIES)
    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT, "Accept": "application/json"}
    async with httpx.AsyncClient(
        transport=transport,
        timeout=timeout or DEFAULT_TIMEOUT,
        headers=headers,
        base_url=base_url or "",
    ) as client:
        yield client
