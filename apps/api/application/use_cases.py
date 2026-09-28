from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel

from apps.api.application.ports import ApiUnitOfWork
from apps.api.domain.models import WorkflowExecution, WorkflowExecutionStatus, WorkflowSnapshot
from packages.contracts import (
    ExecuteWorkflowCommand,
    MessageEnvelope,
    WorkflowCompleted,
    WorkflowDefinition,
    WorkflowFailed,
    WorkflowStarted,
    envelope,
)
from packages.contracts.topics import WORKFLOW_COMMANDS_TOPIC
from packages.observability.metrics import WORKFLOW_SUBMISSIONS, WORKFLOW_TRANSITIONS


class WorkflowNotFoundError(LookupError):
    pass


class WorkflowResultsUnavailableError(RuntimeError):
    def __init__(self, status: WorkflowExecutionStatus) -> None:
        super().__init__(f"workflow results are unavailable while status is '{status.value}'")
        self.status = status


class SubmitWorkflow:
    def __init__(self, uow_factory: Callable[[], ApiUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, workflow: WorkflowDefinition) -> WorkflowExecution:
        now = datetime.now(UTC)
        snapshot = WorkflowSnapshot(
            id=uuid4(),
            name=workflow.name,
            definition=workflow.model_dump(mode="json"),
            created_at=now,
        )
        execution = WorkflowExecution(
            id=uuid4(),
            snapshot_id=snapshot.id,
            name=workflow.name,
            status=WorkflowExecutionStatus.PENDING,
            workflow_input=workflow.input,
            created_at=now,
        )
        async with self._uow_factory() as uow:
            await uow.snapshots.add(snapshot)
            # Persist the parent row before the child row. SQLAlchemy cannot infer
            # mapper ordering here because the repositories intentionally do not
            # expose an ORM relationship across the application boundary.
            await uow.flush()
            await uow.executions.add(execution)
            await uow.commit()
        WORKFLOW_SUBMISSIONS.inc()
        WORKFLOW_TRANSITIONS.labels(service="api", status=execution.status.value).inc()
        return execution


class TriggerWorkflow:
    def __init__(self, uow_factory: Callable[[], ApiUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, execution_id: UUID) -> WorkflowExecution:
        async with self._uow_factory() as uow:
            execution = await uow.executions.get(execution_id, for_update=True)
            if execution is None:
                raise WorkflowNotFoundError(str(execution_id))
            if (
                execution.status is not WorkflowExecutionStatus.PENDING
                or execution.triggered_at is not None
            ):
                return execution
            snapshot = await uow.snapshots.get(execution.snapshot_id)
            if snapshot is None:
                raise RuntimeError(f"snapshot {execution.snapshot_id} is missing")
            now = datetime.now(UTC)
            # Public status remains pending until the Engine publishes
            # WorkflowStarted. triggered_at is the idempotency guard.
            execution.triggered_at = now
            await uow.executions.save(execution)
            command = ExecuteWorkflowCommand(
                execution_id=execution.id,
                workflow_snapshot=snapshot.definition,
                input=execution.workflow_input,
            )
            message = envelope("ExecuteWorkflowCommand", command)
            await uow.outbox.add(
                WORKFLOW_COMMANDS_TOPIC,
                str(execution.id),
                message.message_id,
                message.model_dump(mode="json"),
            )
            await uow.commit()
        WORKFLOW_TRANSITIONS.labels(service="api", status=execution.status.value).inc()
        return execution


class GetWorkflow:
    def __init__(self, uow_factory: Callable[[], ApiUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, execution_id: UUID) -> WorkflowExecution:
        async with self._uow_factory() as uow:
            execution = await uow.executions.get(execution_id)
            if execution is None:
                raise WorkflowNotFoundError(str(execution_id))
            return execution


class GetWorkflowResults(GetWorkflow):
    async def execute(self, execution_id: UUID) -> WorkflowExecution:
        execution = await super().execute(execution_id)
        if execution.status is not WorkflowExecutionStatus.COMPLETED:
            raise WorkflowResultsUnavailableError(execution.status)
        return execution


class ProjectWorkflowEvent:
    def __init__(self, uow_factory: Callable[[], ApiUnitOfWork]) -> None:
        self._uow_factory = uow_factory

    async def execute(self, message: MessageEnvelope[BaseModel]) -> None:
        async with self._uow_factory() as uow:
            is_new = await uow.processed_messages.add_if_new(
                message.message_id, "api-workflow-events"
            )
            if not is_new:
                return
            payload = message.payload
            execution_id = getattr(payload, "execution_id", None)
            if not isinstance(execution_id, UUID):
                raise ValueError("workflow event has no execution_id")
            execution = await uow.executions.get(execution_id, for_update=True)
            if execution is None:
                raise WorkflowNotFoundError(str(execution_id))

            if isinstance(payload, WorkflowStarted):
                if execution.status not in {
                    WorkflowExecutionStatus.COMPLETED,
                    WorkflowExecutionStatus.FAILED,
                }:
                    execution.status = WorkflowExecutionStatus.RUNNING
                    execution.started_at = payload.started_at
            elif isinstance(payload, WorkflowCompleted):
                if execution.status not in {
                    WorkflowExecutionStatus.COMPLETED,
                    WorkflowExecutionStatus.FAILED,
                }:
                    execution.status = WorkflowExecutionStatus.COMPLETED
                    execution.result = payload.result
                    execution.completed_at = payload.completed_at
            elif isinstance(payload, WorkflowFailed):
                if execution.status not in {
                    WorkflowExecutionStatus.COMPLETED,
                    WorkflowExecutionStatus.FAILED,
                }:
                    execution.status = WorkflowExecutionStatus.FAILED
                    execution.error = dict(payload.error)
                    execution.completed_at = payload.failed_at
            else:
                raise ValueError(f"unsupported workflow event payload: {type(payload).__name__}")
            await uow.executions.save(execution)
            await uow.commit()
        WORKFLOW_TRANSITIONS.labels(service="api", status=execution.status.value).inc()
