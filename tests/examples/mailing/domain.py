"""User-owned tables for the executable mailing example."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Uuid,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = [
    "CampaignRecord",
    "campaigns",
    "contacts",
    "create_domain",
    "metadata",
    "record",
    "suppressions",
]

metadata = MetaData()

campaigns = Table(
    "campaigns",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String, nullable=False),
    Column("subject", String, nullable=False),
    Column("html_template", String, nullable=False),
    Column("audience_id", Integer, nullable=False),
    Column("mailbox_ids", JSON, nullable=False),
    Column("scheduled_at", DateTime(timezone=True)),
    Column("status", String, nullable=False, default="draft"),
    Column("pause_reason", String),
    Column("batch_id", Uuid()),
    Column("audience_size", Integer),
    Column("sent", Integer, nullable=False, default=0),
    Column("skipped", Integer, nullable=False, default=0),
    Column("failed", Integer, nullable=False, default=0),
    Column("duplicates", Integer, nullable=False, default=0),
    Column("breakdown", JSON, nullable=False, default=dict),
    Column("progress", Float(), nullable=False, default=0.0),
    Column("progress_seq", Integer, nullable=False, default=0),
    Column("finished_at", DateTime(timezone=True)),
)

contacts = Table(
    "contacts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("audience_id", Integer, nullable=False),
    Column("email", String, nullable=False),
    Column("unsubscribed", Boolean, nullable=False, default=False),
    Column("deleted", Boolean, nullable=False, default=False),
    Index("ix_contacts_audience_id_id", "audience_id", "id"),
)

suppressions = Table(
    "suppressions",
    metadata,
    Column("email", String, primary_key=True),
    Column("reason", String, nullable=False),
)


@dataclass(frozen=True, slots=True)
class CampaignRecord:
    """Typed projection returned to the simulated application UI."""

    id: int
    status: str
    scheduled_at: datetime | None
    pause_reason: str | None
    batch_id: UUID | None
    audience_size: int | None
    sent: int
    skipped: int
    failed: int
    duplicates: int
    breakdown: dict[str, int]
    progress: float
    progress_seq: int
    finished_at: datetime | None


async def create_domain(engine: AsyncEngine) -> None:
    """Create the user-owned tables in the engine's translated schema."""
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all, checkfirst=False)


def record(row: Mapping[str, object]) -> CampaignRecord:
    """Convert one SQLAlchemy mapping to the public domain projection."""
    raw_breakdown = cast("Mapping[str, object]", row["breakdown"])
    return CampaignRecord(
        id=cast("int", row["id"]),
        status=cast("str", row["status"]),
        scheduled_at=cast("datetime | None", row["scheduled_at"]),
        pause_reason=cast("str | None", row["pause_reason"]),
        batch_id=cast("UUID | None", row["batch_id"]),
        audience_size=cast("int | None", row["audience_size"]),
        sent=cast("int", row["sent"]),
        skipped=cast("int", row["skipped"]),
        failed=cast("int", row["failed"]),
        duplicates=cast("int", row["duplicates"]),
        breakdown={name: cast("int", value) for name, value in raw_breakdown.items()},
        progress=cast("float", row["progress"]),
        progress_seq=cast("int", row["progress_seq"]),
        finished_at=cast("datetime | None", row["finished_at"]),
    )
