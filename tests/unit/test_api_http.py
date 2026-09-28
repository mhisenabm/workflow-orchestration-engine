from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from apps.api.domain.models import WorkflowExecution, WorkflowExecutionStatus
from apps.api.interfaces.http import router


class StaticUseCase:
    def __init__(self, execution: WorkflowExecution) -> None:
        self.execution = execution
        self.calls: list[object] = []

    async def execute(self, argument):
        self.calls.append(argument)
        return self.execution


class StaticResultsUseCase:
    def __init__(self, execution: WorkflowExecution) -> None:
        self.execution = execution

    async def execute(self, execution_id: UUID) -> WorkflowExecution:
        assert execution_id == self.execution.id
        return self.execution


def build_app(execution: WorkflowExecution) -> FastAPI:
    app = FastAPI()
    app.state.container = SimpleNamespace(
        submit_workflow=StaticUseCase(execution),
        trigger_workflow=StaticUseCase(execution),
        get_workflow=StaticUseCase(execution),
        get_results=StaticResultsUseCase(execution),
    )
    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_post_routes_match_challenge_contract_exactly() -> None:
    execution = WorkflowExecution(
        id=uuid4(),
        snapshot_id=uuid4(),
        name="Route contract",
        status=WorkflowExecutionStatus.PENDING,
        workflow_input={},
        created_at=datetime.now(UTC),
    )
    transport = httpx.ASGITransport(app=build_app(execution))
    workflow = {
        "name": "Route contract",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {"id": "output", "handler": "output", "dependencies": ["input"]},
            ]
        },
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        submitted = await client.post("/workflow", json=workflow)
        triggered = await client.post(f"/workflow/trigger/{execution.id}")
        old_submit = await client.post("/workflows", json=workflow)
        old_trigger = await client.post(f"/workflows/{execution.id}/trigger")

    assert submitted.status_code == 201
    assert submitted.json()["status"] == "pending"
    assert triggered.status_code == 202
    assert triggered.json()["status"] == "pending"
    assert old_submit.status_code == 404
    assert old_trigger.status_code == 404


@pytest.mark.asyncio
async def test_get_status_uses_pending_running_completed_vocabulary() -> None:
    execution = WorkflowExecution(
        id=uuid4(),
        snapshot_id=uuid4(),
        name="Status contract",
        status=WorkflowExecutionStatus.PENDING,
        workflow_input={},
        created_at=datetime.now(UTC),
    )
    app = build_app(execution)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        pending = await client.get(f"/workflows/{execution.id}")
        execution.status = WorkflowExecutionStatus.RUNNING
        running = await client.get(f"/workflows/{execution.id}")
        execution.status = WorkflowExecutionStatus.COMPLETED
        execution.result = {"ok": True}
        completed = await client.get(f"/workflows/{execution.id}")

    assert pending.json()["status"] == "pending"
    assert running.json()["status"] == "running"
    assert completed.json()["status"] == "completed"
