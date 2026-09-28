from __future__ import annotations

from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from apps.api.application.ports import ApiUnitOfWork
from apps.api.domain.models import WorkflowExecution, WorkflowExecutionStatus, WorkflowSnapshot


class Base(DeclarativeBase):
    pass


class WorkflowSnapshotRow(Base):
    __tablename__ = "workflow_snapshots"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    definition: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WorkflowExecutionRow(Base):
    __tablename__ = "workflow_executions"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    snapshot_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_snapshots.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    workflow_input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    triggered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ApiOutboxRow(Base):
    __tablename__ = "api_outbox"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_api_outbox_message_id"),
        Index("ix_api_outbox_unpublished", "published_at", "created_at"),
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


class ApiProcessedMessageRow(Base):
    __tablename__ = "api_processed_messages"
    __table_args__ = (UniqueConstraint("message_id", "consumer", name="uq_api_processed"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    message_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    consumer: Mapped[str] = mapped_column(String(100), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class SnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, snapshot: WorkflowSnapshot) -> None:
        self._session.add(
            WorkflowSnapshotRow(
                id=snapshot.id,
                name=snapshot.name,
                definition=snapshot.definition,
                created_at=snapshot.created_at,
            )
        )

    async def get(self, snapshot_id: UUID) -> WorkflowSnapshot | None:
        row = await self._session.get(WorkflowSnapshotRow, snapshot_id)
        if row is None:
            return None
        return WorkflowSnapshot(row.id, row.name, row.definition, row.created_at)


class ExecutionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, execution: WorkflowExecution) -> None:
        self._session.add(_execution_to_row(execution))

    async def get(
        self, execution_id: UUID, *, for_update: bool = False
    ) -> WorkflowExecution | None:
        statement = select(WorkflowExecutionRow).where(WorkflowExecutionRow.id == execution_id)
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return _row_to_execution(row) if row is not None else None

    async def save(self, execution: WorkflowExecution) -> None:
        row = await self._session.get(WorkflowExecutionRow, execution.id)
        if row is None:
            raise RuntimeError(f"execution {execution.id} disappeared")
        row.status = execution.status.value
        row.triggered_at = execution.triggered_at
        row.started_at = execution.started_at
        row.completed_at = execution.completed_at
        row.result = execution.result
        row.error = execution.error


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, topic: str, key: str, message_id: UUID, payload: dict[str, object]) -> None:
        self._session.add(
            ApiOutboxRow(
                message_id=message_id,
                topic=topic,
                message_key=key,
                payload=payload,
            )
        )


class ProcessedMessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_if_new(self, message_id: UUID, consumer: str) -> bool:
        statement = (
            insert(ApiProcessedMessageRow)
            .values(message_id=message_id, consumer=consumer)
            .on_conflict_do_nothing(
                index_elements=[
                    ApiProcessedMessageRow.message_id,
                    ApiProcessedMessageRow.consumer,
                ]
            )
            .returning(ApiProcessedMessageRow.id)
        )
        inserted = (await self._session.execute(statement)).scalar_one_or_none()
        return inserted is not None


class SqlAlchemyApiUnitOfWork(ApiUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.snapshots = SnapshotRepository(self._session)
        self.executions = ExecutionRepository(self._session)
        self.outbox = OutboxRepository(self._session)
        self.processed_messages = ProcessedMessageRepository(self._session)
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

    async def flush(self) -> None:
        if self._session is None:
            raise RuntimeError("unit of work is not active")
        await self._session.flush()

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("unit of work is not active")
        await self._session.commit()


def _execution_to_row(execution: WorkflowExecution) -> WorkflowExecutionRow:
    return WorkflowExecutionRow(
        id=execution.id,
        snapshot_id=execution.snapshot_id,
        name=execution.name,
        status=execution.status.value,
        workflow_input=execution.workflow_input,
        result=execution.result,
        error=execution.error,
        created_at=execution.created_at,
        triggered_at=execution.triggered_at,
        started_at=execution.started_at,
        completed_at=execution.completed_at,
    )


def _row_to_execution(row: WorkflowExecutionRow) -> WorkflowExecution:
    return WorkflowExecution(
        id=row.id,
        snapshot_id=row.snapshot_id,
        name=row.name,
        status=WorkflowExecutionStatus(row.status),
        workflow_input=row.workflow_input,
        result=row.result,
        error=row.error,
        created_at=row.created_at,
        triggered_at=row.triggered_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
    )
