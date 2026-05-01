"""SQLAlchemy ``DeclarativeBase`` shared by every ORM model in the codebase.

Living in its own tiny module keeps the metadata import-cycle-free: any
subsystem (``ingest``, ``query``, …) can import :class:`Base` to register
its tables without pulling in the full async engine / session machinery
that lives in :mod:`aeroza.shared.db`.
"""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base; subclass to register a table on the shared metadata."""
