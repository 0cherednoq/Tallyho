"""User-owned tables for the per-recipient export recipe (ARCHITECTURE section 12.9)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Uuid

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["create_delivery_domain", "deliveries", "delivery_campaigns", "recipients"]

# Отдельная MetaData: эталонный сценарий §12 и его таблицы не меняются.
metadata = MetaData()

delivery_campaigns = Table(
    "delivery_campaigns",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("tenant", String, nullable=False),
    # draft → running → settling → completed / completed_with_errors / failed / cancelled
    Column("status", String, nullable=False),
    Column("outcome", String),
    Column("batch_id", Uuid()),
    Column("sent", Integer, nullable=False, default=0),
    Column("failed", Integer, nullable=False, default=0),
    Column("cancelled", Integer, nullable=False, default=0),
    Column("finished_at", DateTime(timezone=True)),
)

recipients = Table(
    "delivery_recipients",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("campaign_id", Integer, primary_key=True),
    Column("email", String, nullable=False),
)

deliveries = Table(
    "mailing_delivery",
    metadata,
    Column("campaign_id", Integer, primary_key=True),
    # Нормализованный адрес — он же ключ Item в этапе send.
    Column("email", String, primary_key=True),
    # pending → sent / failed / cancelled
    Column("status", String, nullable=False),
    Column("reason", String),
)


async def create_delivery_domain(engine: AsyncEngine) -> None:
    """Create the user-owned tables in the engine's translated schema."""
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all, checkfirst=False)
