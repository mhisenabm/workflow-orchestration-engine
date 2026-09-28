from __future__ import annotations

from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self
from uuid import UUID

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from apps.worker.application.ports import WorkerUnitOfWork
from apps.worker.domain.models import ClaimOutcome, ClaimResult, ReceiptStatus, TaskReceipt
from packages.contracts import ExecuteTaskCommand


class Base(DeclarativeBase):
    pass


class WorkerTaskReceiptRow(Base):
    __tablename__ = "worker_task_receipts"

    task_execution_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    execution_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    node_id: Mapped[str] = mapped_column(String(128), nullable=False)
    handler: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    output: Mapped[Any | None] = mapped_column(JSONB)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkerOutboxRow(Base):
    __tablename__ = "worker_outbox"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_worker_outbox_message_id"),
        Index("ix_worker_outbox_unpublished", "published_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class SqlAlchemyWorkerStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim(self, command: ExecuteTaskCommand, lease_expires_at: datetime) -> ClaimResult:
        now = datetime.now(UTC)
        statement = (
            insert(WorkerTaskReceiptRow)
            .values(
                task_execution_id=command.task_execution_id,
                execution_id=command.execution_id,
                node_id=command.node_id,
                handler=command.handler,
                attempt=command.attempt,
                status=ReceiptStatus.PROCESSING.value,
                lease_expires_at=lease_expires_at,
                created_at=now,
            )
            .on_conflict_do_nothing(index_elements=[WorkerTaskReceiptRow.task_execution_id])
            .returning(WorkerTaskReceiptRow.task_execution_id)
        )
        inserted = (await self._session.execute(statement)).scalar_one_or_none()
        row = (
            await self._session.execute(
                select(WorkerTaskReceiptRow)
                .where(WorkerTaskReceiptRow.task_execution_id == command.task_execution_id)
                .with_for_update()
            )
        ).scalar_one()
        receipt = _row_to_receipt(row)
        if inserted is not None:
            return ClaimResult(ClaimOutcome.EXECUTE, receipt)
        if receipt.status in {ReceiptStatus.COMPLETED, ReceiptStatus.FAILED}:
            return ClaimResult(ClaimOutcome.TERMINAL, receipt)
        if receipt.lease_expires_at <= now:
            row.status = ReceiptStatus.FAILED.value
            row.error = {
                "code": "WORKER_EXECUTION_ABANDONED",
                "message": "Worker processing lease expired before completion",
            }
            row.completed_at = now
            return ClaimResult(ClaimOutcome.TERMINAL, _row_to_receipt(row))
        return ClaimResult(ClaimOutcome.BUSY, receipt)

    async def complete(
        self, task_execution_id: UUID, output: Any, completed_at: datetime
    ) -> TaskReceipt:
        row = (
            await self._session.execute(
                select(WorkerTaskReceiptRow)
                .where(WorkerTaskReceiptRow.task_execution_id == task_execution_id)
                .with_for_update()
            )
        ).scalar_one()
        if row.status == ReceiptStatus.PROCESSING.value:
            row.status = ReceiptStatus.COMPLETED.value
            row.output = output
            row.error = None
            row.completed_at = completed_at
        return _row_to_receipt(row)

    async def fail(
        self, task_execution_id: UUID, error: dict[str, Any], failed_at: datetime
    ) -> TaskReceipt:
        row = (
            await self._session.execute(
                select(WorkerTaskReceiptRow)
                .where(WorkerTaskReceiptRow.task_execution_id == task_execution_id)
                .with_for_update()
            )
        ).scalar_one()
        if row.status == ReceiptStatus.PROCESSING.value:
            row.status = ReceiptStatus.FAILED.value
            row.error = error
            row.completed_at = failed_at
        return _row_to_receipt(row)

    async def add_outbox(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None:
        statement = (
            insert(WorkerOutboxRow)
            .values(
                message_id=message_id,
                topic=topic,
                message_key=key,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=[WorkerOutboxRow.message_id])
        )
        await self._session.execute(statement)


class SqlAlchemyWorkerUnitOfWork(WorkerUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.store = SqlAlchemyWorkerStore(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is None:
            return
        if exc is not None:
            await self._session.rollback()
        await self._session.close()

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("unit of work is not active")
        await self._session.commit()


def _row_to_receipt(row: WorkerTaskReceiptRow) -> TaskReceipt:
    return TaskReceipt(
        task_execution_id=row.task_execution_id,
        execution_id=row.execution_id,
        node_id=row.node_id,
        handler=row.handler,
        attempt=row.attempt,
        status=ReceiptStatus(row.status),
        lease_expires_at=row.lease_expires_at,
        output=row.output,
        error=row.error,
        created_at=row.created_at,
        completed_at=row.completed_at,
    )
