import asyncio
from uuid import uuid4

import pytest

from apps.worker.application.executor import WorkerTaskExecutor
from apps.worker.application.registry import HandlerRegistry
from packages.contracts import ExecuteTaskCommand, envelope
from tests.unit.fakes import MemoryWorkerState, MemoryWorkerUow


class CountingHandler:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, config, inputs):
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return {"ok": True}


@pytest.mark.asyncio
async def test_duplicate_task_message_executes_handler_once() -> None:
    state = MemoryWorkerState()
    handler = CountingHandler()
    registry = HandlerRegistry()
    registry.register("test", handler)
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    command = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="b",
        handler="test",
        attempt=1,
    )
    message = envelope("ExecuteTaskCommand", command)

    first = asyncio.create_task(executor.process(message))
    await handler.started.wait()
    assert await executor.process(message) is False
    handler.release.set()
    assert await first is True
    assert await executor.process(message) is True

    assert handler.calls == 1
    receipt = state.receipts[command.task_execution_id]
    assert receipt.output == {"ok": True}
    terminal_events = [
        payload
        for _, _, payload in state.outbox.values()
        if payload["message_type"] == "TaskCompleted"
    ]
    assert len(terminal_events) == 1


@pytest.mark.asyncio
async def test_new_retry_task_execution_id_runs_again() -> None:
    state = MemoryWorkerState()

    class ImmediateHandler:
        def __init__(self) -> None:
            self.calls = 0

        async def execute(self, config, inputs):
            self.calls += 1
            return self.calls

    handler = ImmediateHandler()
    registry = HandlerRegistry()
    registry.register("test", handler)
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    execution_id = uuid4()
    for attempt in (1, 2):
        command = ExecuteTaskCommand(
            execution_id=execution_id,
            task_execution_id=uuid4(),
            node_id="b",
            handler="test",
            attempt=attempt,
        )
        assert await executor.process(envelope("ExecuteTaskCommand", command))
    assert handler.calls == 2


@pytest.mark.asyncio
async def test_expired_processing_receipt_fails_same_attempt_without_reexecution() -> None:
    from datetime import UTC, datetime, timedelta

    state = MemoryWorkerState()
    handler = CountingHandler()
    registry = HandlerRegistry()
    registry.register("test", handler)
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    command = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="b",
        handler="test",
        attempt=1,
    )
    message = envelope("ExecuteTaskCommand", command)

    first = asyncio.create_task(executor.process(message))
    await handler.started.wait()
    state.receipts[command.task_execution_id].lease_expires_at = datetime.now(UTC) - timedelta(
        seconds=1
    )
    assert await executor.process(message) is True
    handler.release.set()
    assert await first is True

    assert handler.calls == 1
    receipt = state.receipts[command.task_execution_id]
    assert receipt.status.value == "failed"
    assert receipt.error["code"] == "WORKER_EXECUTION_ABANDONED"


@pytest.mark.asyncio
async def test_duplicate_task_id_with_different_identity_is_rejected() -> None:
    state = MemoryWorkerState()

    class ImmediateHandler:
        async def execute(self, config, inputs):
            return {"ok": True}

    registry = HandlerRegistry()
    registry.register("test", ImmediateHandler())
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    task_execution_id = uuid4()
    original = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=task_execution_id,
        node_id="b",
        handler="test",
        attempt=1,
    )
    assert await executor.process(envelope("ExecuteTaskCommand", original)) is True

    conflicting = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=task_execution_id,
        node_id="different-node",
        handler="test",
        attempt=1,
    )
    with pytest.raises(ValueError, match="identity does not match"):
        await executor.process(envelope("ExecuteTaskCommand", conflicting))


@pytest.mark.asyncio
async def test_non_json_handler_output_becomes_task_failure() -> None:
    state = MemoryWorkerState()

    class InvalidOutputHandler:
        async def execute(self, config, inputs):
            return object()

    registry = HandlerRegistry()
    registry.register("test", InvalidOutputHandler())
    executor = WorkerTaskExecutor(lambda: MemoryWorkerUow(state), registry, 60)
    command = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="b",
        handler="test",
        attempt=1,
    )

    assert await executor.process(envelope("ExecuteTaskCommand", command)) is True
    receipt = state.receipts[command.task_execution_id]
    assert receipt.status.value == "failed"
    assert receipt.error is not None
    assert receipt.error["code"] == "INVALID_TASK_OUTPUT"
