"""Persistence for the MRMS file catalog: idempotent upsert on ``key``.

S3 listing is naturally idempotent — the same object can be re-listed
many times without changing — so the upsert mirrors the alerts pattern:
``ON CONFLICT (key) DO UPDATE`` with a ``WHERE col IS DISTINCT FROM …``
clause to skip no-op writes, and Postgres's ``xmax = 0`` system column
in the ``RETURNING`` clause to count inserts vs updates in one round trip.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import func, literal_column, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_models import MrmsFileRow

log = structlog.get_logger(__name__)

_MUTABLE_COLUMNS: tuple[str, ...] = (
    "product",
    "level",
    "valid_at",
    "size_bytes",
    "etag",
)


@dataclass(frozen=True, slots=True)
class MrmsUpsertResult:
    inserted_keys: tuple[str, ...]
    updated_keys: tuple[str, ...]

    @property
    def inserted(self) -> int:
        return len(self.inserted_keys)

    @property
    def updated(self) -> int:
        return len(self.updated_keys)

    @property
    def total(self) -> int:
        return self.inserted + self.updated


_EMPTY_RESULT: MrmsUpsertResult = MrmsUpsertResult(inserted_keys=(), updated_keys=())


async def upsert_mrms_files(session: AsyncSession, files: Iterable[MrmsFile]) -> MrmsUpsertResult:
    """Upsert ``files`` by S3 key. Returns per-row insert/update outcomes.

    Empty input is a no-op. The session is **not** committed — callers own
    the transaction boundary.
    """
    rows = [_to_row_dict(f) for f in files]
    if not rows:
        return _EMPTY_RESULT

    insert_stmt = pg_insert(MrmsFileRow).values(rows)
    update_set: dict[str, Any] = {col: insert_stmt.excluded[col] for col in _MUTABLE_COLUMNS}
    update_set["updated_at"] = func.now()

    upsert_stmt: Any = insert_stmt.on_conflict_do_update(
        index_elements=[MrmsFileRow.key],
        set_=update_set,
        where=_changed_predicate(insert_stmt),
    ).returning(
        MrmsFileRow.key,
        literal_column("(xmax = 0)").label("inserted"),
    )

    result = await session.execute(upsert_stmt)
    affected = result.all()
    inserted_keys = tuple(row.key for row in affected if row.inserted)
    updated_keys = tuple(row.key for row in affected if not row.inserted)
    return MrmsUpsertResult(inserted_keys=inserted_keys, updated_keys=updated_keys)


def _changed_predicate(stmt: Any) -> Any:
    excluded = stmt.excluded
    table = MrmsFileRow.__table__
    return or_(*(table.c[col].is_distinct_from(excluded[col]) for col in _MUTABLE_COLUMNS))


def _to_row_dict(file: MrmsFile) -> dict[str, Any]:
    return {
        "key": file.key,
        "product": file.product,
        "level": file.level,
        "valid_at": file.valid_at,
        "size_bytes": file.size_bytes,
        "etag": file.etag,
    }
