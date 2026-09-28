from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.engine.application.orchestrator import EngineOrchestrator
from apps.engine.domain.models import TaskStatus, WorkflowRuntimeStatus
from apps.worker.application.executor import WorkerTaskExecutor
from apps.worker.application.registry import HandlerRegistry
from packages.contracts import ExecuteTaskCommand, ExecuteWorkflowCommand, TaskFailed, envelope
from tests.unit.fakes import (
    MemoryEngineState,
    MemoryEngineUow,
    MemoryWorkerState,
    MemoryWorkerUow,
)


@pytest.mark.asyncio
async def test_engine_creates_retry_then_fails_after_exhaustion() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "retry",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "task",
                    "handler": "llm_service",
                    "dependencies": ["input"],
                    "config": {"prompt": "x"},
                    "retry": {"max_attempts": 2, "delay_seconds": 0},
                },
                {"id": "output", "handler": "output", "dependencies": ["task"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    first = next(t for t in state.tasks.values() if t.node_id == "task")
    failed = TaskFailed(
        execution_id=execution_id,
        task_execution_id=first.id,
        node_id="task",
        attempt=1,
        error={"code": "X", "message": "fail"},
        failed_at=datetime.now(UTC),
    )
    await orchestrator.process_task_event(envelope("TaskFailed", failed))
    second = max((t for t in state.tasks.values() if t.node_id == "task"), key=lambda t: t.attempt)
    assert second.status is TaskStatus.RETRYING
    await orchestrator.dispatch_retry(second.id)
    assert second.status is TaskStatus.QUEUED

    failed2 = failed.model_copy(
        update={"task_execution_id": second.id, "attempt": 2, "failed_at": datetime.now(UTC)}
    )
    await orchestrator.process_task_event(envelope("TaskFailed", failed2))
    assert state.runtimes[execution_id].status is WorkflowRuntimeStatus.FAILED


@pytest.mark.asyncio
async def test_worker_timeout_creates_failed_receipt() -> None:
    class SlowHandler:
        async def execute(self, config, inputs):
            import asyncio

            await asyncio.sleep(2)
            return "late"

    state = MemoryWorkerState()
    registry = HandlerRegistry()
    registry.register("slow", SlowHandler())
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    command = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="slow",
        handler="slow",
        attempt=1,
        timeout=1,
    )
    await executor.process(envelope("ExecuteTaskCommand", command))
    receipt = state.receipts[command.task_execution_id]
    assert receipt.error["code"] == "TASK_TIMEOUT"
