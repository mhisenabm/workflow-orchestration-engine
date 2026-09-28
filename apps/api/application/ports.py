from __future__ import annotations

from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from apps.api.domain.models import WorkflowExecution, WorkflowSnapshot


class WorkflowSnapshotRepository(Protocol):
    async def add(self, snapshot: WorkflowSnapshot) -> None: ...
    async def get(self, snapshot_id: UUID) -> WorkflowSnapshot | None: ...


class WorkflowExecutionRepository(Protocol):
    async def add(self, execution: WorkflowExecution) -> None: ...
    async def get(
        self, execution_id: UUID, *, for_update: bool = False
    ) -> WorkflowExecution | None: ...
    async def save(self, execution: WorkflowExecution) -> None: ...


class ApiOutbox(Protocol):
    async def add(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None: ...


class ProcessedMessageRepository(Protocol):
    async def add_if_new(self, message_id: UUID, consumer: str) -> bool: ...


class ApiUnitOfWork(Protocol):
    snapshots: WorkflowSnapshotRepository
    executions: WorkflowExecutionRepository
    outbox: ApiOutbox
    processed_messages: ProcessedMessageRepository

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def flush(self) -> None: ...
    async def commit(self) -> None: ...
