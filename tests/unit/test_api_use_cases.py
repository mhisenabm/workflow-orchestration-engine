from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from typing import Self
from uuid import UUID

import pytest

from apps.api.application.use_cases import (
    GetWorkflowResults,
    ProjectWorkflowEvent,
    SubmitWorkflow,
    TriggerWorkflow,
    WorkflowResultsUnavailableError,
)
from apps.api.domain.models import WorkflowExecution, WorkflowSnapshot
from packages.contracts import (
    WorkflowCompleted,
    WorkflowDefinition,
    WorkflowStarted,
    envelope,
)


class ApiState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.snapshots: dict[UUID, WorkflowSnapshot] = {}
        self.executions: dict[UUID, WorkflowExecution] = {}
        self.outbox: dict[UUID, dict[str, object]] = {}
        self.processed: set[tuple[UUID, str]] = set()
        self.operations: list[str] = []


class SnapshotRepo:
    def __init__(self, state: ApiState) -> None:
        self.state = state

    async def add(self, snapshot: WorkflowSnapshot) -> None:
        self.state.operations.append("snapshot:add")
        self.state.snapshots[snapshot.id] = snapshot

    async def get(self, snapshot_id: UUID) -> WorkflowSnapshot | None:
        return self.state.snapshots.get(snapshot_id)


class ExecutionRepo:
    def __init__(self, state: ApiState) -> None:
        self.state = state

    async def add(self, execution: WorkflowExecution) -> None:
        self.state.operations.append("execution:add")
        self.state.executions[execution.id] = execution

    async def get(
        self, execution_id: UUID, *, for_update: bool = False
    ) -> WorkflowExecution | None:
        return self.state.executions.get(execution_id)

    async def save(self, execution: WorkflowExecution) -> None:
        self.state.executions[execution.id] = execution


class OutboxRepo:
    def __init__(self, state: ApiState) -> None:
        self.state = state

    async def add(self, topic: str, key: str, message_id: UUID, payload: dict[str, object]) -> None:
        self.state.outbox.setdefault(message_id, deepcopy(payload))


class ProcessedRepo:
    def __init__(self, state: ApiState) -> None:
        self.state = state

    async def add_if_new(self, message_id: UUID, consumer: str) -> bool:
        key = (message_id, consumer)
        if key in self.state.processed:
            return False
        self.state.processed.add(key)
        return True


class ApiUow:
    def __init__(self, state: ApiState) -> None:
        self.state = state
        self.snapshots = SnapshotRepo(state)
        self.executions = ExecutionRepo(state)
        self.outbox = OutboxRepo(state)
        self.processed_messages = ProcessedRepo(state)

    async def __aenter__(self) -> Self:
        await self.state.lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.state.lock.release()

    async def flush(self) -> None:
        self.state.operations.append("flush")

    async def commit(self) -> None:
        self.state.operations.append("commit")


WORKFLOW = WorkflowDefinition.model_validate(
    {
        "name": "Simple",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "task",
                    "handler": "llm_service",
                    "dependencies": ["input"],
                    "config": {"prompt": "hello"},
                },
                {"id": "output", "handler": "output", "dependencies": ["task"]},
            ]
        },
    }
)


@pytest.mark.asyncio
async def test_submit_creates_snapshot_and_execution() -> None:
    state = ApiState()
    execution = await SubmitWorkflow(lambda: ApiUow(state)).execute(WORKFLOW)
    assert execution.status.value == "pending"
    assert execution.id in state.executions
    assert execution.snapshot_id in state.snapshots
    assert state.operations == ["snapshot:add", "flush", "execution:add", "commit"]


@pytest.mark.asyncio
async def test_trigger_is_idempotent_without_extra_request_parameters() -> None:
    state = ApiState()
    submit = SubmitWorkflow(lambda: ApiUow(state))
    trigger = TriggerWorkflow(lambda: ApiUow(state))
    execution = await submit.execute(WORKFLOW)

    first = await trigger.execute(execution.id)
    second = await trigger.execute(execution.id)

    assert first.status.value == "pending"
    assert second.status.value == "pending"
    assert first.triggered_at is not None
    assert second.triggered_at == first.triggered_at
    commands = [m for m in state.outbox.values() if m["message_type"] == "ExecuteWorkflowCommand"]
    assert len(commands) == 1


@pytest.mark.asyncio
async def test_results_before_completion_are_unavailable() -> None:
    state = ApiState()
    execution = await SubmitWorkflow(lambda: ApiUow(state)).execute(WORKFLOW)
    with pytest.raises(WorkflowResultsUnavailableError):
        await GetWorkflowResults(lambda: ApiUow(state)).execute(execution.id)


@pytest.mark.asyncio
async def test_workflow_event_projection_uses_public_status_vocabulary() -> None:
    state = ApiState()
    execution = await SubmitWorkflow(lambda: ApiUow(state)).execute(WORKFLOW)
    projector = ProjectWorkflowEvent(lambda: ApiUow(state))
    started_at = datetime.now(UTC)

    await projector.execute(
        envelope(
            "WorkflowStarted",
            WorkflowStarted(execution_id=execution.id, started_at=started_at),
        )
    )
    assert state.executions[execution.id].status.value == "running"
    assert state.executions[execution.id].started_at == started_at

    completed_at = datetime.now(UTC)
    await projector.execute(
        envelope(
            "WorkflowCompleted",
            WorkflowCompleted(
                execution_id=execution.id,
                result={"task": {"ok": True}},
                completed_at=completed_at,
            ),
        )
    )
    projected = state.executions[execution.id]
    assert projected.status.value == "completed"
    assert projected.result == {"task": {"ok": True}}
    assert projected.completed_at == completed_at


@pytest.mark.asyncio
async def test_first_terminal_workflow_event_is_authoritative() -> None:
    state = ApiState()
    execution = await SubmitWorkflow(lambda: ApiUow(state)).execute(WORKFLOW)
    projector = ProjectWorkflowEvent(lambda: ApiUow(state))

    await projector.execute(
        envelope(
            "WorkflowCompleted",
            WorkflowCompleted(
                execution_id=execution.id,
                result={"value": "first"},
                completed_at=datetime.now(UTC),
            ),
        )
    )
    await projector.execute(
        envelope(
            "WorkflowCompleted",
            WorkflowCompleted(
                execution_id=execution.id,
                result={"value": "conflicting-late-event"},
                completed_at=datetime.now(UTC),
            ),
        )
    )

    assert state.executions[execution.id].result == {"value": "first"}
