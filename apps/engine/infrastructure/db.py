from __future__ import annotations

from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self
from uuid import UUID

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from apps.engine.application.ports import EngineUnitOfWork
from apps.engine.domain.models import (
    TaskExecution,
    TaskStatus,
    WorkflowRuntime,
    WorkflowRuntimeStatus,
)


class Base(DeclarativeBase):
    pass


class WorkflowRuntimeRow(Base):
    __tablename__ = "workflow_runtime"

    execution_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    workflow_snapshot: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    workflow_input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[Any | None] = mapped_column(JSONB)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class TaskExecutionRow(Base):
    __tablename__ = "task_executions"
    __table_args__ = (
        UniqueConstraint("execution_id", "node_id", "attempt", name="uq_task_attempt"),
        Index("ix_task_retry_due", "status", "available_at"),
        Index("ix_task_execution_node", "execution_id", "node_id"),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("workflow_runtime.execution_id"), nullable=False
    )
    node_id: Mapped[str] = mapped_column(String(128), nullable=False)
    handler: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    retry_max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_delay_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    timeout: Mapped[int | None] = mapped_column(Integer)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class TaskResultRow(Base):
    __tablename__ = "task_results"
    __table_args__ = (
        UniqueConstraint("task_execution_id", name="uq_task_result_task"),
        Index("ix_task_result_execution_node", "execution_id", "node_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("task_executions.id"), nullable=False
    )
    execution_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    node_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    output: Mapped[Any] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )


class EngineOutboxRow(Base):
    __tablename__ = "engine_outbox"
    __table_args__ = (
        UniqueConstraint("message_id", name="uq_engine_outbox_message_id"),
        Index("ix_engine_outbox_unpublished", "published_at", "created_at"),
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


class EngineProcessedMessageRow(Base):
    __tablename__ = "engine_processed_messages"
    __table_args__ = (UniqueConstraint("message_id", "consumer", name="uq_engine_processed"),)

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


class SqlAlchemyEngineStore:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def workflow_exists(self, execution_id: UUID) -> bool:
        return await self._session.get(WorkflowRuntimeRow, execution_id) is not None

    async def add_runtime(self, runtime: WorkflowRuntime) -> None:
        self._session.add(_runtime_to_row(runtime))

    async def flush(self) -> None:
        await self._session.flush()

    async def get_runtime(
        self, execution_id: UUID, *, for_update: bool = False
    ) -> WorkflowRuntime | None:
        statement = select(WorkflowRuntimeRow).where(
            WorkflowRuntimeRow.execution_id == execution_id
        )
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return _row_to_runtime(row) if row is not None else None

    async def save_runtime(self, runtime: WorkflowRuntime) -> None:
        row = await self._session.get(WorkflowRuntimeRow, runtime.execution_id)
        if row is None:
            raise RuntimeError(f"runtime {runtime.execution_id} disappeared")
        row.status = runtime.status.value
        row.completed_at = runtime.completed_at
        row.result = runtime.result
        row.error = runtime.error

    async def add_task(self, task: TaskExecution) -> None:
        self._session.add(_task_to_row(task))
        await self._session.flush()

    async def get_task(
        self, task_execution_id: UUID, *, for_update: bool = False
    ) -> TaskExecution | None:
        statement = select(TaskExecutionRow).where(TaskExecutionRow.id == task_execution_id)
        if for_update:
            statement = statement.with_for_update()
        row = (await self._session.execute(statement)).scalar_one_or_none()
        return _row_to_task(row) if row is not None else None

    async def get_latest_tasks(self, execution_id: UUID) -> dict[str, TaskExecution]:
        subquery = (
            select(
                TaskExecutionRow.node_id,
                func.max(TaskExecutionRow.attempt).label("max_attempt"),
            )
            .where(TaskExecutionRow.execution_id == execution_id)
            .group_by(TaskExecutionRow.node_id)
            .subquery()
        )
        rows = (
            (
                await self._session.execute(
                    select(TaskExecutionRow)
                    .join(
                        subquery,
                        (TaskExecutionRow.node_id == subquery.c.node_id)
                        & (TaskExecutionRow.attempt == subquery.c.max_attempt),
                    )
                    .where(TaskExecutionRow.execution_id == execution_id)
                )
            )
            .scalars()
            .all()
        )
        return {row.node_id: _row_to_task(row) for row in rows}

    async def save_task(self, task: TaskExecution) -> None:
        row = await self._session.get(TaskExecutionRow, task.id)
        if row is None:
            raise RuntimeError(f"task {task.id} disappeared")
        row.status = task.status.value
        row.config = task.config
        row.inputs = task.inputs
        row.available_at = task.available_at
        row.started_at = task.started_at
        row.completed_at = task.completed_at
        row.error = task.error

    async def add_result(self, task: TaskExecution, output: Any) -> None:
        statement = (
            insert(TaskResultRow)
            .values(
                task_execution_id=task.id,
                execution_id=task.execution_id,
                node_id=task.node_id,
                attempt=task.attempt,
                output=output,
            )
            .on_conflict_do_nothing(index_elements=[TaskResultRow.task_execution_id])
        )
        await self._session.execute(statement)

    async def get_outputs(self, execution_id: UUID) -> dict[str, Any]:
        subquery = (
            select(TaskResultRow.node_id, func.max(TaskResultRow.attempt).label("max_attempt"))
            .where(TaskResultRow.execution_id == execution_id)
            .group_by(TaskResultRow.node_id)
            .subquery()
        )
        rows = (
            (
                await self._session.execute(
                    select(TaskResultRow)
                    .join(
                        subquery,
                        (TaskResultRow.node_id == subquery.c.node_id)
                        & (TaskResultRow.attempt == subquery.c.max_attempt),
                    )
                    .where(TaskResultRow.execution_id == execution_id)
                )
            )
            .scalars()
            .all()
        )
        return {row.node_id: row.output for row in rows}

    async def add_outbox(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None:
        statement = (
            insert(EngineOutboxRow)
            .values(
                message_id=message_id,
                topic=topic,
                message_key=key,
                payload=payload,
            )
            .on_conflict_do_nothing(index_elements=[EngineOutboxRow.message_id])
        )
        await self._session.execute(statement)

    async def add_processed_message(self, message_id: UUID, consumer: str) -> bool:
        statement = (
            insert(EngineProcessedMessageRow)
            .values(message_id=message_id, consumer=consumer)
            .on_conflict_do_nothing(
                index_elements=[
                    EngineProcessedMessageRow.message_id,
                    EngineProcessedMessageRow.consumer,
                ]
            )
            .returning(EngineProcessedMessageRow.id)
        )
        inserted = (await self._session.execute(statement)).scalar_one_or_none()
        return inserted is not None


class SqlAlchemyEngineUnitOfWork(EngineUnitOfWork):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.store = SqlAlchemyEngineStore(self._session)
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


class SqlAlchemyDueRetryFinder:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def find_due_retry_ids(self, now: datetime, limit: int = 100) -> list[UUID]:
        async with self._session_factory() as session:
            rows = await session.scalars(
                select(TaskExecutionRow.id)
                .join(
                    WorkflowRuntimeRow,
                    WorkflowRuntimeRow.execution_id == TaskExecutionRow.execution_id,
                )
                .where(
                    TaskExecutionRow.status == TaskStatus.RETRYING.value,
                    TaskExecutionRow.available_at <= now,
                    WorkflowRuntimeRow.status == WorkflowRuntimeStatus.RUNNING.value,
                )
                .order_by(TaskExecutionRow.available_at)
                .limit(limit)
            )
            return list(rows)


def _runtime_to_row(runtime: WorkflowRuntime) -> WorkflowRuntimeRow:
    return WorkflowRuntimeRow(
        execution_id=runtime.execution_id,
        workflow_snapshot=runtime.workflow_snapshot,
        workflow_input=runtime.workflow_input,
        status=runtime.status.value,
        started_at=runtime.started_at,
        completed_at=runtime.completed_at,
        result=runtime.result,
        error=runtime.error,
    )


def _row_to_runtime(row: WorkflowRuntimeRow) -> WorkflowRuntime:
    return WorkflowRuntime(
        execution_id=row.execution_id,
        workflow_snapshot=row.workflow_snapshot,
        workflow_input=row.workflow_input,
        status=WorkflowRuntimeStatus(row.status),
        started_at=row.started_at,
        completed_at=row.completed_at,
        result=row.result,
        error=row.error,
    )


def _task_to_row(task: TaskExecution) -> TaskExecutionRow:
    return TaskExecutionRow(
        id=task.id,
        execution_id=task.execution_id,
        node_id=task.node_id,
        handler=task.handler,
        status=task.status.value,
        attempt=task.attempt,
        config=task.config,
        inputs=task.inputs,
        retry_max_attempts=task.retry_max_attempts,
        retry_delay_seconds=task.retry_delay_seconds,
        timeout=task.timeout,
        available_at=task.available_at,
        created_at=task.created_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        error=task.error,
    )


def _row_to_task(row: TaskExecutionRow) -> TaskExecution:
    return TaskExecution(
        id=row.id,
        execution_id=row.execution_id,
        node_id=row.node_id,
        handler=row.handler,
        status=TaskStatus(row.status),
        attempt=row.attempt,
        config=row.config,
        inputs=row.inputs,
        retry_max_attempts=row.retry_max_attempts,
        retry_delay_seconds=row.retry_delay_seconds,
        timeout=row.timeout,
        available_at=row.available_at,
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        error=row.error,
    )
