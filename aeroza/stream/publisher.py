"""Publisher abstractions for ingest events.

One Protocol per domain rather than a single generic ``Publisher[T]``: the
subject taxonomy and payload encoding are part of each publisher's
identity, and a generic surface would push that detail into call sites.

Each domain ships three implementations:

- ``Nats…Publisher`` — production. Lives in :mod:`aeroza.stream.nats` to
  keep the live driver out of the import graph here.
- ``Null…Publisher`` — drops everything. Safe default for backfills /
  environments without a broker.
- ``InMemory…Publisher`` — captures payloads for tests.
"""

from __future__ import annotations

from typing import Protocol

import structlog

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_zarr import MrmsGridLocator
from aeroza.ingest.nws_alerts import Alert

log = structlog.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Alerts                                                                       #
# --------------------------------------------------------------------------- #


class AlertPublisher(Protocol):
    """Emits one event per newly-observed alert."""

    async def publish_new_alert(self, alert: Alert) -> None:  # pragma: no cover - interface
        ...


class NullAlertPublisher:
    """Drops every event. Useful as a default when streaming is disabled."""

    async def publish_new_alert(self, alert: Alert) -> None:
        log.debug("publisher.null.drop", alert_id=alert.id)


class InMemoryAlertPublisher:
    """Captures published alerts in a list — for tests only."""

    def __init__(self) -> None:
        self._published: list[Alert] = []

    @property
    def published(self) -> tuple[Alert, ...]:
        return tuple(self._published)

    @property
    def published_ids(self) -> tuple[str, ...]:
        return tuple(alert.id for alert in self._published)

    async def publish_new_alert(self, alert: Alert) -> None:
        self._published.append(alert)

    def clear(self) -> None:
        self._published.clear()


# --------------------------------------------------------------------------- #
# MRMS file catalog                                                            #
# --------------------------------------------------------------------------- #


class MrmsFilePublisher(Protocol):
    """Emits one event per newly-observed MRMS file."""

    async def publish_new_file(self, file: MrmsFile) -> None:  # pragma: no cover - interface
        ...


class NullMrmsFilePublisher:
    """Drops every event."""

    async def publish_new_file(self, file: MrmsFile) -> None:
        log.debug("publisher.null.drop", mrms_key=file.key)


class InMemoryMrmsFilePublisher:
    """Captures published MRMS files in a list — for tests only."""

    def __init__(self) -> None:
        self._published: list[MrmsFile] = []

    @property
    def published(self) -> tuple[MrmsFile, ...]:
        return tuple(self._published)

    @property
    def published_keys(self) -> tuple[str, ...]:
        return tuple(file.key for file in self._published)

    async def publish_new_file(self, file: MrmsFile) -> None:
        self._published.append(file)

    def clear(self) -> None:
        self._published.clear()


# --------------------------------------------------------------------------- #
# Materialised MRMS grids                                                      #
# --------------------------------------------------------------------------- #


class MrmsGridPublisher(Protocol):
    """Emits one event per newly-materialised MRMS grid.

    The semantic is "first time this file_key has been turned into a Zarr
    store" — re-materialisations of an updated source file do not produce
    a new event (mirrors the file-catalog publisher's "newly-discovered"
    semantic).
    """

    async def publish_new_grid(
        self, locator: MrmsGridLocator
    ) -> None:  # pragma: no cover - interface
        ...


class NullMrmsGridPublisher:
    """Drops every event."""

    async def publish_new_grid(self, locator: MrmsGridLocator) -> None:
        log.debug("publisher.null.drop", file_key=locator.file_key)


class InMemoryMrmsGridPublisher:
    """Captures published locators in a list — for tests only."""

    def __init__(self) -> None:
        self._published: list[MrmsGridLocator] = []

    @property
    def published(self) -> tuple[MrmsGridLocator, ...]:
        return tuple(self._published)

    @property
    def published_keys(self) -> tuple[str, ...]:
        return tuple(loc.file_key for loc in self._published)

    async def publish_new_grid(self, locator: MrmsGridLocator) -> None:
        self._published.append(locator)

    def clear(self) -> None:
        self._published.clear()
