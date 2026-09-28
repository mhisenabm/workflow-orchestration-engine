from __future__ import annotations

from datetime import datetime
from types import TracebackType
from typing import Any, Protocol, Self
from uuid import UUID

from apps.worker.domain.models import ClaimResult, TaskReceipt
from packages.contracts import ExecuteTaskCommand


class TaskHandler(Protocol):
    async def execute(self, config: dict[str, Any], inputs: dict[str, Any]) -> Any: ...


class WorkerStore(Protocol):
    async def claim(
        self, command: ExecuteTaskCommand, lease_expires_at: datetime
    ) -> ClaimResult: ...
    async def complete(
        self, task_execution_id: UUID, output: Any, completed_at: datetime
    ) -> TaskReceipt: ...
    async def fail(
        self, task_execution_id: UUID, error: dict[str, Any], failed_at: datetime
    ) -> TaskReceipt: ...
    async def add_outbox(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None: ...


class WorkerUnitOfWork(Protocol):
    store: WorkerStore

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...
