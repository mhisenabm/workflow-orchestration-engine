import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from apps.engine.application.orchestrator import EngineOrchestrator
from apps.engine.domain.models import TaskStatus, WorkflowRuntimeStatus
from packages.contracts import (
    ExecuteWorkflowCommand,
    TaskCompleted,
    TaskStarted,
    envelope,
)
from tests.unit.fakes import MemoryEngineState, MemoryEngineUow


def task_commands(state: MemoryEngineState, node_id: str | None = None) -> list[dict]:
    messages = [
        payload
        for _, _, payload in state.outbox.values()
        if payload["message_type"] == "ExecuteTaskCommand"
    ]
    if node_id is not None:
        messages = [m for m in messages if m["payload"]["node_id"] == node_id]
    return messages


def task_for(state: MemoryEngineState, execution_id: UUID, node_id: str):
    matches = [
        task
        for task in state.tasks.values()
        if task.execution_id == execution_id and task.node_id == node_id
    ]
    return max(matches, key=lambda task: task.attempt)


async def complete(
    orchestrator: EngineOrchestrator,
    execution_id: UUID,
    task_id: UUID,
    node_id: str,
    output,
    *,
    message_id: UUID | None = None,
    completed_at: datetime | None = None,
) -> None:
    event = envelope(
        "TaskCompleted",
        TaskCompleted(
            execution_id=execution_id,
            task_execution_id=task_id,
            node_id=node_id,
            attempt=1,
            output=output,
            completed_at=completed_at or datetime.now(UTC),
        ),
        message_id=message_id,
    )
    await orchestrator.process_task_event(event)


@pytest.mark.asyncio
async def test_scenario_a_linear_data_passing() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Linear",
        "input": {"customer_id": 42},
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://customers/{{ input.customer_id }}"},
                },
                {
                    "id": "c",
                    "handler": "llm_service",
                    "dependencies": ["b"],
                    "config": {
                        "prompt": "Customer {{ b.data.name }} has id {{ input.customer_id }}"
                    },
                },
                {"id": "output", "handler": "output", "dependencies": ["c"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(
                execution_id=execution_id,
                workflow_snapshot=workflow,
                input={"customer_id": 42},
            ),
        )
    )

    b_command = task_commands(state, "b")[0]
    assert b_command["payload"]["config"]["url"] == "mock://customers/42"
    b_task = task_for(state, execution_id, "b")
    await complete(orchestrator, execution_id, b_task.id, "b", {"data": {"name": "John"}})

    c_command = task_commands(state, "c")[0]
    assert c_command["payload"]["config"]["prompt"] == "Customer John has id 42"
    c_task = task_for(state, execution_id, "c")
    await complete(orchestrator, execution_id, c_task.id, "c", "summary")

    runtime = state.runtimes[execution_id]
    assert runtime.status is WorkflowRuntimeStatus.COMPLETED
    assert runtime.result == {"c": "summary"}


@pytest.mark.asyncio
async def test_scenario_b_fan_out_fan_in() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Fan out",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://b"},
                },
                {
                    "id": "c",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://c"},
                },
                {"id": "output", "handler": "output", "dependencies": ["b", "c"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    assert {m["payload"]["node_id"] for m in task_commands(state)} == {"b", "c"}

    b_task = task_for(state, execution_id, "b")
    c_task = task_for(state, execution_id, "c")
    await complete(orchestrator, execution_id, b_task.id, "b", {"value": "B"})
    assert state.runtimes[execution_id].status is WorkflowRuntimeStatus.RUNNING
    assert task_for(state, execution_id, "output").status is TaskStatus.PENDING

    await complete(orchestrator, execution_id, c_task.id, "c", {"value": "C"})
    assert state.runtimes[execution_id].result == {
        "b": {"value": "B"},
        "c": {"value": "C"},
    }


@pytest.mark.asyncio
async def test_scenario_c_concurrent_fan_in_schedules_d_once() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Race safe",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://b"},
                },
                {
                    "id": "c",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://c"},
                },
                {
                    "id": "d",
                    "handler": "llm_service",
                    "dependencies": ["b", "c"],
                    "config": {"prompt": "{{ b.value }} + {{ c.value }}"},
                },
                {"id": "output", "handler": "output", "dependencies": ["d"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    b_task = task_for(state, execution_id, "b")
    c_task = task_for(state, execution_id, "c")

    same_completion_time = datetime.now(UTC).replace(microsecond=123000)
    await asyncio.gather(
        complete(
            orchestrator,
            execution_id,
            b_task.id,
            "b",
            {"value": "B"},
            completed_at=same_completion_time,
        ),
        complete(
            orchestrator,
            execution_id,
            c_task.id,
            "c",
            {"value": "C"},
            completed_at=same_completion_time,
        ),
    )

    d_tasks = [
        task
        for task in state.tasks.values()
        if task.execution_id == execution_id and task.node_id == "d" and task.attempt == 1
    ]
    assert len(d_tasks) == 1
    assert d_tasks[0].status is TaskStatus.QUEUED
    d_commands = task_commands(state, "d")
    assert len(d_commands) == 1
    assert d_commands[0]["payload"]["config"]["prompt"] == "B + C"

    # A redelivered completion with a different message id still cannot dispatch D again.
    await complete(orchestrator, execution_id, b_task.id, "b", {"value": "B"})
    assert len(task_commands(state, "d")) == 1


@pytest.mark.asyncio
async def test_task_event_identity_mismatch_is_rejected() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Identity",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://b"},
                },
                {"id": "output", "handler": "output", "dependencies": ["b"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    task = task_for(state, execution_id, "b")
    mismatched = envelope(
        "TaskCompleted",
        TaskCompleted(
            execution_id=execution_id,
            task_execution_id=task.id,
            node_id="wrong-node",
            attempt=1,
            output={"value": "wrong"},
            completed_at=datetime.now(UTC),
        ),
    )

    with pytest.raises(ValueError, match="identity does not match"):
        await orchestrator.process_task_event(mismatched)

    assert task_for(state, execution_id, "b").status is TaskStatus.QUEUED
    assert task.id not in state.results


@pytest.mark.asyncio
async def test_terminal_event_cannot_complete_a_pending_task() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Pending guard",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://b"},
                },
                {
                    "id": "c",
                    "handler": "llm_service",
                    "dependencies": ["b"],
                    "config": {"prompt": "{{ b.value }}"},
                },
                {"id": "output", "handler": "output", "dependencies": ["c"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    c_task = task_for(state, execution_id, "c")
    assert c_task.status is TaskStatus.PENDING

    await complete(orchestrator, execution_id, c_task.id, "c", "impossible")

    assert task_for(state, execution_id, "c").status is TaskStatus.PENDING
    assert c_task.id not in state.results
    assert state.runtimes[execution_id].status is WorkflowRuntimeStatus.RUNNING


@pytest.mark.asyncio
async def test_completion_before_started_event_is_safe_and_late_started_is_ignored() -> None:
    state = MemoryEngineState()
    orchestrator = EngineOrchestrator(lambda: MemoryEngineUow(state))
    execution_id = uuid4()
    workflow = {
        "name": "Out-of-order worker events",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://b"},
                },
                {"id": "output", "handler": "output", "dependencies": ["b"]},
            ]
        },
    }
    await orchestrator.start_workflow(
        envelope(
            "ExecuteWorkflowCommand",
            ExecuteWorkflowCommand(execution_id=execution_id, workflow_snapshot=workflow, input={}),
        )
    )
    task = task_for(state, execution_id, "b")

    # Multiple outbox publishers may publish a terminal event before TaskStarted.
    await complete(orchestrator, execution_id, task.id, "b", {"value": "B"})
    assert task_for(state, execution_id, "b").status is TaskStatus.COMPLETED
    assert state.runtimes[execution_id].status is WorkflowRuntimeStatus.COMPLETED

    await orchestrator.process_task_event(
        envelope(
            "TaskStarted",
            TaskStarted(
                execution_id=execution_id,
                task_execution_id=task.id,
                node_id="b",
                attempt=1,
                started_at=datetime.now(UTC),
            ),
        )
    )
    assert task_for(state, execution_id, "b").status is TaskStatus.COMPLETED
    assert state.runtimes[execution_id].status is WorkflowRuntimeStatus.COMPLETED
