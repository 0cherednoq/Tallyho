"""User-owned import table for the catalog example."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import JSON, Column, Float, Integer, MetaData, String, Table, Uuid

if TYPE_CHECKING:
    from collections.abc import Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["CatalogRecord", "catalog_imports", "create_domain", "metadata", "record"]

metadata = MetaData()

catalog_imports = Table(
    "catalog_imports",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("batch_id", Uuid()),
    Column("stages", JSON, nullable=False, default=dict),
    Column("progress", Float(), nullable=False, default=0.0),
    Column("progress_seq", Integer, nullable=False, default=0),
)


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    """Domain projection rendered by the simulated UI."""

    id: int
    status: str
    batch_id: UUID | None
    stages: dict[str, dict[str, object]]
    progress: float
    progress_seq: int


async def create_domain(engine: AsyncEngine) -> None:
    """Create catalog tables in the translated user schema."""
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all, checkfirst=False)


def record(row: Mapping[str, object]) -> CatalogRecord:
    """Convert one SQLAlchemy row mapping into a typed projection."""
    raw_stages = cast("Mapping[str, Mapping[str, object]]", row["stages"])
    return CatalogRecord(
        id=cast("int", row["id"]),
        status=cast("str", row["status"]),
        batch_id=cast("UUID | None", row["batch_id"]),
        stages={name: dict(values) for name, values in raw_stages.items()},
        progress=cast("float", row["progress"]),
        progress_seq=cast("int", row["progress_seq"]),
    )
