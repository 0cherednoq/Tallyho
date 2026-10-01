"""User-owned S1/S2/S3 tables and shared hook journal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    Uuid,
    func,
)

if TYPE_CHECKING:
    from sqlalchemy.sql.base import ReadOnlyColumnCollection

    DomainTable = Table[ReadOnlyColumnCollection[str, Column[object]]]

__all__ = [
    "DomainTable",
    "DomainTables",
    "audience",
    "build_domain",
    "campaigns",
    "cards",
    "catalog_runs",
    "deliveries",
    "hook_log",
    "invoice_files",
    "invoices",
    "metadata",
    "pdf_files",
    "task_log",
]

metadata = MetaData()

invoices = Table(
    "invoices",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("batch_status", String, nullable=False, server_default="running"),
    Column("batch_id", Uuid()),
    Column("progress_done", Integer, nullable=False, server_default="0"),
    Column("progress_found", Integer, nullable=False, server_default="0"),
    Column("policy_breaches", Integer, nullable=False, server_default="0"),
)

invoice_files = Table(
    "invoice_files",
    metadata,
    Column("item_id", Uuid(), primary_key=True),
    Column("invoice_id", Integer, ForeignKey("invoices.id"), nullable=False),
    Column("bytes", LargeBinary, nullable=False),
)

campaigns = Table(
    "acceptance_campaigns",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("batch_status", String, nullable=False, server_default="running"),
    Column("batch_id", Uuid()),
    Column("progress_done", Integer, nullable=False, server_default="0"),
    Column("progress_found", Integer, nullable=False, server_default="0"),
    Column("policy_breaches", Integer, nullable=False, server_default="0"),
)

audience = Table(
    "acceptance_audience",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("campaign_id", Integer, ForeignKey("acceptance_campaigns.id"), nullable=False),
    Column("email", String, nullable=False),
)

deliveries = Table(
    "deliveries",
    metadata,
    Column("item_id", Uuid(), primary_key=True),
    Column("campaign_id", Integer, ForeignKey("acceptance_campaigns.id"), nullable=False),
    Column("email", String, nullable=False),
    Column("label", String, nullable=False),
)

catalog_runs = Table(
    "acceptance_catalog_runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("status", String, nullable=False),
    Column("batch_status", String, nullable=False, server_default="running"),
    Column("batch_id", Uuid()),
    Column("progress_done", Integer, nullable=False, server_default="0"),
    Column("progress_found", Integer, nullable=False, server_default="0"),
    Column("policy_breaches", Integer, nullable=False, server_default="0"),
)

cards = Table(
    "cards",
    metadata,
    Column("item_id", Uuid(), primary_key=True),
    Column("run_id", Integer, ForeignKey("acceptance_catalog_runs.id"), nullable=False),
    Column("url", String, nullable=False),
    Column("status", Integer, nullable=False),
    UniqueConstraint("run_id", "url", name="uq_cards_run_url"),
)

pdf_files = Table(
    "pdf_files",
    metadata,
    Column("item_id", Uuid(), primary_key=True),
    Column("run_id", Integer, ForeignKey("acceptance_catalog_runs.id"), nullable=False),
    Column("url", String, nullable=False),
    Column("bytes", LargeBinary, nullable=False),
    UniqueConstraint("run_id", "url", name="uq_pdf_files_run_url"),
)

task_log = Table(
    "acceptance_task_log",
    metadata,
    Column("item_id", Uuid(), primary_key=True),
    Column("scenario", String, nullable=False),
    Column("task", String, nullable=False),
    Column("status", String, nullable=False),
)

hook_log = Table(
    "hook_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("batch_id", Uuid(), nullable=False),
    Column("hook", String, nullable=False),
    Column("seq", BigInteger, nullable=False),
    Column("state", Integer, nullable=False),
    Column("progress_done", BigInteger, nullable=False),
    Column("progress_found", BigInteger, nullable=False),
    Column("txid", BigInteger, nullable=False),
    Column("at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("batch_id", "hook", "seq", name="uq_hook_log_event"),
)


@dataclass(frozen=True, slots=True)
class DomainTables:
    """One acceptance application's user tables in an explicit PostgreSQL schema."""

    metadata: MetaData
    invoices: DomainTable
    invoice_files: DomainTable
    campaigns: DomainTable
    audience: DomainTable
    deliveries: DomainTable
    catalog_runs: DomainTable
    cards: DomainTable
    pdf_files: DomainTable
    task_log: DomainTable
    hook_log: DomainTable


def build_domain(schema: str) -> DomainTables:
    """Clone table prototypes into ``schema`` so complete_in can also map th_* tables."""
    scoped = MetaData()
    by_name = {
        table.name: table.to_metadata(scoped, schema=schema) for table in metadata.sorted_tables
    }
    return DomainTables(
        metadata=scoped,
        invoices=by_name[invoices.name],
        invoice_files=by_name[invoice_files.name],
        campaigns=by_name[campaigns.name],
        audience=by_name[audience.name],
        deliveries=by_name[deliveries.name],
        catalog_runs=by_name[catalog_runs.name],
        cards=by_name[cards.name],
        pdf_files=by_name[pdf_files.name],
        task_log=by_name[task_log.name],
        hook_log=by_name[hook_log.name],
    )
