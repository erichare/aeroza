"""Disk + DB retention for MRMS observations, nowcasts, and alerts.

The materialiser writes a Zarr store per MRMS frame and the nowcast
worker writes a Zarr per (observation × horizon × algorithm). Both
arrive every ~2 minutes and have no automatic cleanup, so the Railway
volume fills up. This module is the cleanup side: a long-lived worker
(see :mod:`aeroza.cli.prune_mrms`) ticks every few minutes, removes
old Zarr stores from disk, and prunes the catalog rows that reference
them via Postgres FK cascades.
"""

from aeroza.retention.worker import (
    PruneResult,
    prune_expired_alerts_once,
    prune_old_mrms_once,
)

__all__ = [
    "PruneResult",
    "prune_expired_alerts_once",
    "prune_old_mrms_once",
]
