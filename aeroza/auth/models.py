"""SQLAlchemy ORM model for ``api_keys``.

Mirrors the ``20260502_1300_add_api_keys`` migration. The two
status values (``active`` / ``revoked``) are enforced both here (CHECK
constraint in the migration) and in the typed
:data:`API_KEY_STATUSES` tuple — drift is caught by the integration
tests on the first wrong-value insert.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Final

from sqlalchemy import CheckConstraint, DateTime, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column

from aeroza.shared.base import Base

API_KEYS_TABLE: Final[str] = "api_keys"

API_KEY_STATUSES: Final[tuple[str, ...]] = ("active", "revoked")
API_KEY_DEFAULT_RATE_LIMIT_CLASS: Final[str] = "default"


class ApiKeyRow(Base):
    __tablename__ = API_KEYS_TABLE

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    prefix: Mapped[str] = mapped_column(Text, nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        default=list,
        server_default="{}",
    )
    rate_limit_class: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=API_KEY_DEFAULT_RATE_LIMIT_CLASS,
        server_default=API_KEY_DEFAULT_RATE_LIMIT_CLASS,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="active",
        server_default="active",
    )
    owner: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("prefix", name="uq_api_keys_prefix"),
        CheckConstraint(
            "status IN ('active', 'revoked')",
            name="api_keys_status_valid",
        ),
        CheckConstraint(
            "char_length(prefix) >= 4",
            name="api_keys_prefix_min_length",
        ),
        CheckConstraint(
            "char_length(key_hash) = 64",
            name="api_keys_key_hash_sha256_length",
        ),
    )


__all__ = [
    "API_KEYS_TABLE",
    "API_KEY_DEFAULT_RATE_LIMIT_CLASS",
    "API_KEY_STATUSES",
    "ApiKeyRow",
]
