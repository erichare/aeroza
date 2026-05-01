"""Persistence for the materialised MRMS grids catalog.

Single-row upserts on ``file_key``: re-materialising an updated source
file replaces the previous Zarr metadata in place. ``materialised_at``
is bumped on every real change via the same ``IS DISTINCT FROM`` filter
the alerts and file-catalog upserts use.
"""

from __future__ import annotations

import json
from typing import Any, Final

import structlog
from sqlalchemy import func, literal_column, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms_grids_models import MrmsGridRow
from aeroza.ingest.mrms_zarr import MrmsGridLocator, locator_to_row_dict

log = structlog.get_logger(__name__)

_MUTABLE_COLUMNS: Final[tuple[str, ...]] = (
    "zarr_uri",
    "variable",
    "dims_json",
    "shape_json",
    "dtype",
    "nbytes",
)


async def upsert_mrms_grid(session: AsyncSession, locator: MrmsGridLocator) -> bool:
    """Upsert the row for ``locator``. Returns ``True`` if it was a fresh insert.

    The session is **not** committed; the caller owns the transaction.
    No-op upserts (locator identical to the existing row) are filtered
    out by the ``WHERE`` clause and don't bump ``materialised_at``.
    """
    row = locator_to_row_dict(locator)
    insert_stmt = pg_insert(MrmsGridRow).values(row)
    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}
    update_set["materialised_at"] = func.now()

    upsert_stmt: Any = insert_stmt.on_conflict_do_update(
        index_elements=[MrmsGridRow.file_key],
        set_=update_set,
        where=_changed_predicate(insert_stmt),
    ).returning(
        MrmsGridRow.file_key,
        literal_column("(xmax = 0)").label("inserted"),
    )

    result = await session.execute(upsert_stmt)
    affected = result.first()
    if affected is None:
        # Existing row was unchanged — WHERE filtered the update.
        log.debug("mrms.grids.upsert.noop", file_key=locator.file_key)
        return False
    log.info(
        "mrms.grids.upsert",
        file_key=locator.file_key,
        inserted=bool(affected.inserted),
        zarr_uri=locator.zarr_uri,
    )
    return bool(affected.inserted)


def _changed_predicate(stmt: Any) -> Any:
    excluded = stmt.excluded
    table = MrmsGridRow.__table__
    return or_(*(table.c[col].is_distinct_from(excluded[col]) for col in _MUTABLE_COLUMNS))


def parse_dims_shape(row: MrmsGridRow) -> tuple[tuple[str, ...], tuple[int, ...]]:
    """Recover the original ``dims`` and ``shape`` tuples from a stored row.

    JSONB columns come back as Python ``list`` from SQLAlchemy + asyncpg;
    callers wanting the original tuple shape go through this helper rather
    than re-implementing the JSON parsing.
    """
    dims_raw = row.dims_json
    shape_raw = row.shape_json
    dims = json.loads(dims_raw) if isinstance(dims_raw, str) else dims_raw
    shape = json.loads(shape_raw) if isinstance(shape_raw, str) else shape_raw
    return tuple(str(d) for d in dims), tuple(int(s) for s in shape)
