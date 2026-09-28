from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, JsonValue

from apps.api.application.use_cases import (
    WorkflowNotFoundError,
    WorkflowResultsUnavailableError,
)
from apps.api.domain.models import WorkflowExecution
from packages.contracts import WorkflowDefinition

router = APIRouter()


class ExecutionCreatedResponse(BaseModel):
    execution_id: UUID
    status: str


class ExecutionStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    execution_id: UUID
    name: str
    status: str
    created_at: datetime
    triggered_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    error: dict[str, Any] | None


class ExecutionResultsResponse(BaseModel):
    execution_id: UUID
    status: str
    result: JsonValue


def _container(request: Request) -> Any:
    return request.app.state.container


@router.post(
    "/workflow", response_model=ExecutionCreatedResponse, status_code=status.HTTP_201_CREATED
)
async def submit_workflow(
    workflow: WorkflowDefinition,
    container: Any = Depends(_container),
) -> ExecutionCreatedResponse:
    execution = await container.submit_workflow.execute(workflow)
    return ExecutionCreatedResponse(execution_id=execution.id, status=execution.status.value)


@router.post(
    "/workflow/trigger/{execution_id}",
    response_model=ExecutionCreatedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_workflow(
    execution_id: UUID,
    container: Any = Depends(_container),
) -> ExecutionCreatedResponse:
    try:
        execution = await container.trigger_workflow.execute(execution_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail="workflow execution not found") from exc
    return ExecutionCreatedResponse(execution_id=execution.id, status=execution.status.value)


@router.get("/workflows/{execution_id}", response_model=ExecutionStatusResponse)
async def get_workflow(
    execution_id: UUID,
    container: Any = Depends(_container),
) -> ExecutionStatusResponse:
    try:
        execution = await container.get_workflow.execute(execution_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail="workflow execution not found") from exc
    return _status_response(execution)


@router.get("/workflows/{execution_id}/results", response_model=ExecutionResultsResponse)
async def get_workflow_results(
    execution_id: UUID,
    container: Any = Depends(_container),
) -> ExecutionResultsResponse:
    try:
        execution = await container.get_results.execute(execution_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail="workflow execution not found") from exc
    except WorkflowResultsUnavailableError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ExecutionResultsResponse(
        execution_id=execution.id,
        status=execution.status.value,
        result=execution.result,
    )


def _status_response(execution: WorkflowExecution) -> ExecutionStatusResponse:
    return ExecutionStatusResponse(
        execution_id=execution.id,
        name=execution.name,
        status=execution.status.value,
        created_at=execution.created_at,
        triggered_at=execution.triggered_at,
        started_at=execution.started_at,
        completed_at=execution.completed_at,
        error=execution.error,
    )
