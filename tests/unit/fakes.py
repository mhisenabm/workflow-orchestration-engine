from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Self
from uuid import UUID

from apps.engine.domain.models import TaskExecution, WorkflowRuntime
from apps.worker.domain.models import ClaimOutcome, ClaimResult, ReceiptStatus, TaskReceipt
from packages.contracts import ExecuteTaskCommand


class MemoryEngineState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.runtimes: dict[UUID, WorkflowRuntime] = {}
        self.tasks: dict[UUID, TaskExecution] = {}
        self.results: dict[UUID, Any] = {}
        self.outbox: dict[UUID, tuple[str, str, dict[str, object]]] = {}
        self.processed: set[tuple[UUID, str]] = set()


class MemoryEngineStore:
    def __init__(self, state: MemoryEngineState) -> None:
        self.state = state

    async def workflow_exists(self, execution_id: UUID) -> bool:
        return execution_id in self.state.runtimes

    async def add_runtime(self, runtime: WorkflowRuntime) -> None:
        self.state.runtimes[runtime.execution_id] = runtime

    async def flush(self) -> None:
        return None

    async def get_runtime(self, execution_id: UUID, *, for_update: bool = False):
        return self.state.runtimes.get(execution_id)

    async def save_runtime(self, runtime: WorkflowRuntime) -> None:
        self.state.runtimes[runtime.execution_id] = runtime

    async def add_task(self, task: TaskExecution) -> None:
        logical = (task.execution_id, task.node_id, task.attempt)
        if any(
            (t.execution_id, t.node_id, t.attempt) == logical for t in self.state.tasks.values()
        ):
            raise ValueError(f"duplicate task attempt: {logical}")
        self.state.tasks[task.id] = task

    async def get_task(self, task_execution_id: UUID, *, for_update: bool = False):
        return self.state.tasks.get(task_execution_id)

    async def get_latest_tasks(self, execution_id: UUID) -> dict[str, TaskExecution]:
        result: dict[str, TaskExecution] = {}
        for task in self.state.tasks.values():
            if task.execution_id != execution_id:
                continue
            current = result.get(task.node_id)
            if current is None or task.attempt > current.attempt:
                result[task.node_id] = task
        return result

    async def save_task(self, task: TaskExecution) -> None:
        self.state.tasks[task.id] = task

    async def add_result(self, task: TaskExecution, output: Any) -> None:
        self.state.results.setdefault(task.id, deepcopy(output))

    async def get_outputs(self, execution_id: UUID) -> dict[str, Any]:
        latest = await self.get_latest_tasks(execution_id)
        result: dict[str, Any] = {}
        for node_id, task in latest.items():
            if task.id in self.state.results:
                result[node_id] = deepcopy(self.state.results[task.id])
        return result

    async def add_outbox(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None:
        self.state.outbox.setdefault(message_id, (topic, key, deepcopy(payload)))

    async def add_processed_message(self, message_id: UUID, consumer: str) -> bool:
        key = (message_id, consumer)
        if key in self.state.processed:
            return False
        self.state.processed.add(key)
        return True


class MemoryEngineUow:
    def __init__(self, state: MemoryEngineState) -> None:
        self.state = state
        self.store = MemoryEngineStore(state)

    async def __aenter__(self) -> Self:
        await self.state.lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.state.lock.release()

    async def commit(self) -> None:
        return None


class MemoryWorkerState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.receipts: dict[UUID, TaskReceipt] = {}
        self.outbox: dict[UUID, tuple[str, str, dict[str, object]]] = {}


class MemoryWorkerStore:
    def __init__(self, state: MemoryWorkerState) -> None:
        self.state = state

    async def claim(self, command: ExecuteTaskCommand, lease_expires_at: datetime) -> ClaimResult:
        now = datetime.now(UTC)
        receipt = self.state.receipts.get(command.task_execution_id)
        if receipt is None:
            receipt = TaskReceipt(
                task_execution_id=command.task_execution_id,
                execution_id=command.execution_id,
                node_id=command.node_id,
                handler=command.handler,
                attempt=command.attempt,
                status=ReceiptStatus.PROCESSING,
                lease_expires_at=lease_expires_at,
                created_at=now,
            )
            self.state.receipts[command.task_execution_id] = receipt
            return ClaimResult(ClaimOutcome.EXECUTE, receipt)
        if receipt.status in {ReceiptStatus.COMPLETED, ReceiptStatus.FAILED}:
            return ClaimResult(ClaimOutcome.TERMINAL, receipt)
        if receipt.lease_expires_at <= now:
            receipt.status = ReceiptStatus.FAILED
            receipt.error = {
                "code": "WORKER_EXECUTION_ABANDONED",
                "message": "Worker processing lease expired before completion",
            }
            receipt.completed_at = now
            return ClaimResult(ClaimOutcome.TERMINAL, receipt)
        return ClaimResult(ClaimOutcome.BUSY, receipt)

    async def complete(self, task_execution_id: UUID, output: Any, completed_at: datetime):
        receipt = self.state.receipts[task_execution_id]
        if receipt.status is ReceiptStatus.PROCESSING:
            receipt.status = ReceiptStatus.COMPLETED
            receipt.output = deepcopy(output)
            receipt.completed_at = completed_at
        return receipt

    async def fail(self, task_execution_id: UUID, error: dict[str, Any], failed_at: datetime):
        receipt = self.state.receipts[task_execution_id]
        if receipt.status is ReceiptStatus.PROCESSING:
            receipt.status = ReceiptStatus.FAILED
            receipt.error = deepcopy(error)
            receipt.completed_at = failed_at
        return receipt

    async def add_outbox(
        self, topic: str, key: str, message_id: UUID, payload: dict[str, object]
    ) -> None:
        self.state.outbox.setdefault(message_id, (topic, key, deepcopy(payload)))


class MemoryWorkerUow:
    def __init__(self, state: MemoryWorkerState) -> None:
        self.state = state
        self.store = MemoryWorkerStore(state)

    async def __aenter__(self) -> Self:
        await self.state.lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.state.lock.release()

    async def commit(self) -> None:
        return None
