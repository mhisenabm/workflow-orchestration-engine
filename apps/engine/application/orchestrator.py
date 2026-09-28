from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel

from apps.engine.application.ports import EngineStore, EngineUnitOfWork
from apps.engine.application.template_resolver import TemplateResolutionError, TemplateResolver
from apps.engine.domain.models import (
    TaskExecution,
    TaskStatus,
    WorkflowRuntime,
    WorkflowRuntimeStatus,
)
from packages.contracts import (
    ExecuteTaskCommand,
    ExecuteWorkflowCommand,
    MessageEnvelope,
    TaskCompleted,
    TaskFailed,
    TaskStarted,
    WorkflowCompleted,
    WorkflowDefinition,
    WorkflowFailed,
    WorkflowStarted,
    envelope,
)
from packages.contracts.topics import TASK_COMMANDS_TOPIC, WORKFLOW_EVENTS_TOPIC
from packages.observability.metrics import TASK_TRANSITIONS, WORKFLOW_TRANSITIONS


class EngineOrchestrator:
    def __init__(
        self,
        uow_factory: Callable[[], EngineUnitOfWork],
        resolver: TemplateResolver | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._resolver = resolver or TemplateResolver()

    async def start_workflow(self, message: MessageEnvelope[ExecuteWorkflowCommand]) -> None:
        command = message.payload
        async with self._uow_factory() as uow:
            if not await uow.store.add_processed_message(message.message_id, "workflow-commands"):
                return
            if await uow.store.workflow_exists(command.execution_id):
                await uow.commit()
                return
            workflow = WorkflowDefinition.model_validate(command.workflow_snapshot)
            now = datetime.now(UTC)
            runtime = WorkflowRuntime(
                execution_id=command.execution_id,
                workflow_snapshot=workflow.model_dump(mode="json"),
                workflow_input=command.input,
                status=WorkflowRuntimeStatus.RUNNING,
                started_at=now,
            )
            await uow.store.add_runtime(runtime)
            # workflow_runtime is the parent of every task_executions row.
            # Flush it first so a single transaction cannot violate the FK.
            await uow.store.flush()
            for node in workflow.dag.nodes:
                retry = node.retry
                task = TaskExecution(
                    id=uuid4(),
                    execution_id=command.execution_id,
                    node_id=node.id,
                    handler=node.handler,
                    status=TaskStatus.PENDING,
                    attempt=1,
                    config=dict(node.config),
                    inputs=dict(node.inputs),
                    retry_max_attempts=retry.max_attempts if retry else 1,
                    retry_delay_seconds=retry.delay_seconds if retry else 0,
                    timeout=node.timeout,
                    available_at=now,
                    created_at=now,
                )
                if node.handler == "input":
                    task.status = TaskStatus.COMPLETED
                    task.started_at = now
                    task.completed_at = now
                await uow.store.add_task(task)
                if node.handler == "input":
                    await uow.store.add_result(task, command.input)
            started = envelope(
                "WorkflowStarted",
                WorkflowStarted(execution_id=command.execution_id, started_at=now),
                message_id=_event_id("WorkflowStarted", command.execution_id),
            )
            await uow.store.add_outbox(
                WORKFLOW_EVENTS_TOPIC,
                str(command.execution_id),
                started.message_id,
                started.model_dump(mode="json"),
            )
            await self._advance(uow.store, runtime, workflow)
            await uow.commit()
        WORKFLOW_TRANSITIONS.labels(service="engine", status="running").inc()

    async def process_task_event(self, message: MessageEnvelope[BaseModel]) -> None:
        payload = message.payload
        execution_id = getattr(payload, "execution_id", None)
        if not isinstance(execution_id, UUID):
            raise ValueError("task event has no execution_id")
        async with self._uow_factory() as uow:
            if not await uow.store.add_processed_message(message.message_id, "task-events"):
                return
            runtime = await uow.store.get_runtime(execution_id, for_update=True)
            if runtime is None:
                raise LookupError(f"workflow runtime {execution_id} not found")
            if runtime.status is not WorkflowRuntimeStatus.RUNNING:
                await uow.commit()
                return
            task_id = getattr(payload, "task_execution_id", None)
            if not isinstance(task_id, UUID):
                raise ValueError("task event has no task_execution_id")
            task = await uow.store.get_task(task_id, for_update=True)
            if task is None:
                raise LookupError(f"task execution {task_id} not found")
            _validate_task_event_identity(task, payload)

            if isinstance(payload, TaskStarted):
                if task.status is TaskStatus.QUEUED:
                    task.status = TaskStatus.RUNNING
                    task.started_at = payload.started_at
                    await uow.store.save_task(task)
                    TASK_TRANSITIONS.labels(
                        service="engine", status="running", handler=task.handler
                    ).inc()
            elif isinstance(payload, TaskCompleted):
                if task.status in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                    task.status = TaskStatus.COMPLETED
                    task.completed_at = payload.completed_at
                    task.error = None
                    await uow.store.save_task(task)
                    await uow.store.add_result(task, payload.output)
                    workflow = WorkflowDefinition.model_validate(runtime.workflow_snapshot)
                    await self._advance(uow.store, runtime, workflow)
                    TASK_TRANSITIONS.labels(
                        service="engine", status="completed", handler=task.handler
                    ).inc()
            elif isinstance(payload, TaskFailed):
                if task.status in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                    task.status = TaskStatus.FAILED
                    task.completed_at = payload.failed_at
                    task.error = dict(payload.error)
                    await uow.store.save_task(task)
                    await self._retry_or_fail(uow.store, runtime, task)
                    TASK_TRANSITIONS.labels(
                        service="engine", status="failed", handler=task.handler
                    ).inc()
            else:
                raise ValueError(f"unsupported task event: {type(payload).__name__}")
            await uow.commit()

    async def dispatch_retry(self, task_execution_id: UUID) -> None:
        async with self._uow_factory() as uow:
            task = await uow.store.get_task(task_execution_id, for_update=True)
            if task is None or task.status is not TaskStatus.RETRYING:
                return
            runtime = await uow.store.get_runtime(task.execution_id, for_update=True)
            if runtime is None or runtime.status is not WorkflowRuntimeStatus.RUNNING:
                return
            if task.available_at > datetime.now(UTC):
                return
            workflow = WorkflowDefinition.model_validate(runtime.workflow_snapshot)
            await self._queue_task(uow.store, runtime, workflow, task)
            await uow.commit()

    async def _advance(
        self,
        store: EngineStore,
        runtime: WorkflowRuntime,
        workflow: WorkflowDefinition,
    ) -> None:
        if runtime.status is not WorkflowRuntimeStatus.RUNNING:
            return
        made_progress = True
        while made_progress and runtime.status is WorkflowRuntimeStatus.RUNNING:
            made_progress = False
            latest = await store.get_latest_tasks(runtime.execution_id)
            outputs = await store.get_outputs(runtime.execution_id)
            for node in workflow.dag.nodes:
                task = latest[node.id]
                if task.status is not TaskStatus.PENDING:
                    continue
                if not all(
                    latest[dependency].status is TaskStatus.COMPLETED
                    for dependency in node.dependencies
                ):
                    continue
                if node.handler == "output":
                    result = {dependency: outputs[dependency] for dependency in node.dependencies}
                    now = datetime.now(UTC)
                    task.status = TaskStatus.COMPLETED
                    task.started_at = now
                    task.completed_at = now
                    await store.save_task(task)
                    await store.add_result(task, result)
                    runtime.status = WorkflowRuntimeStatus.COMPLETED
                    runtime.result = result
                    runtime.completed_at = now
                    await store.save_runtime(runtime)
                    completed = envelope(
                        "WorkflowCompleted",
                        WorkflowCompleted(
                            execution_id=runtime.execution_id,
                            result=result,
                            completed_at=now,
                        ),
                        message_id=_event_id("WorkflowCompleted", runtime.execution_id),
                    )
                    await store.add_outbox(
                        WORKFLOW_EVENTS_TOPIC,
                        str(runtime.execution_id),
                        completed.message_id,
                        completed.model_dump(mode="json"),
                    )
                    WORKFLOW_TRANSITIONS.labels(service="engine", status="completed").inc()
                    made_progress = True
                    break
                await self._queue_task(store, runtime, workflow, task)
                made_progress = True

    async def _queue_task(
        self,
        store: EngineStore,
        runtime: WorkflowRuntime,
        workflow: WorkflowDefinition,
        task: TaskExecution,
    ) -> None:
        node = workflow.node_map()[task.node_id]
        outputs = await store.get_outputs(runtime.execution_id)
        allowed_nodes = workflow.ancestors_of(node.id)
        try:
            resolved_config = self._resolver.resolve(node.config, outputs, allowed_nodes)
            resolved_inputs = self._resolver.resolve(node.inputs, outputs, allowed_nodes)
        except TemplateResolutionError as exc:
            now = datetime.now(UTC)
            task.status = TaskStatus.FAILED
            task.completed_at = now
            task.error = {
                "code": "TEMPLATE_RESOLUTION_FAILED",
                "message": str(exc),
                "template": exc.template,
                "node_id": task.node_id,
            }
            await store.save_task(task)
            await self._retry_or_fail(store, runtime, task)
            return

        task.config = resolved_config
        task.inputs = resolved_inputs
        task.status = TaskStatus.QUEUED
        await store.save_task(task)
        command = ExecuteTaskCommand(
            execution_id=task.execution_id,
            task_execution_id=task.id,
            node_id=task.node_id,
            handler=task.handler,
            config=resolved_config,
            inputs=resolved_inputs,
            attempt=task.attempt,
            timeout=task.timeout,
        )
        message = envelope(
            "ExecuteTaskCommand",
            command,
            message_id=_event_id("ExecuteTaskCommand", task.id),
        )
        await store.add_outbox(
            TASK_COMMANDS_TOPIC,
            str(task.id),
            message.message_id,
            message.model_dump(mode="json"),
        )
        TASK_TRANSITIONS.labels(service="engine", status="queued", handler=task.handler).inc()

    async def _retry_or_fail(
        self,
        store: EngineStore,
        runtime: WorkflowRuntime,
        failed_task: TaskExecution,
    ) -> None:
        if failed_task.attempt < failed_task.retry_max_attempts:
            now = datetime.now(UTC)
            retry = TaskExecution(
                id=uuid4(),
                execution_id=failed_task.execution_id,
                node_id=failed_task.node_id,
                handler=failed_task.handler,
                status=TaskStatus.RETRYING,
                attempt=failed_task.attempt + 1,
                config=failed_task.config,
                inputs=failed_task.inputs,
                retry_max_attempts=failed_task.retry_max_attempts,
                retry_delay_seconds=failed_task.retry_delay_seconds,
                timeout=failed_task.timeout,
                available_at=now + timedelta(seconds=failed_task.retry_delay_seconds),
                created_at=now,
            )
            await store.add_task(retry)
            TASK_TRANSITIONS.labels(
                service="engine", status="retrying", handler=retry.handler
            ).inc()
            return

        now = datetime.now(UTC)
        runtime.status = WorkflowRuntimeStatus.FAILED
        runtime.completed_at = now
        runtime.error = failed_task.error or {
            "code": "TASK_FAILED",
            "message": f"task '{failed_task.node_id}' failed",
        }
        await store.save_runtime(runtime)
        failed = envelope(
            "WorkflowFailed",
            WorkflowFailed(
                execution_id=runtime.execution_id,
                error=runtime.error,
                failed_at=now,
            ),
            message_id=_event_id("WorkflowFailed", runtime.execution_id),
        )
        await store.add_outbox(
            WORKFLOW_EVENTS_TOPIC,
            str(runtime.execution_id),
            failed.message_id,
            failed.model_dump(mode="json"),
        )
        WORKFLOW_TRANSITIONS.labels(service="engine", status="failed").inc()


def _validate_task_event_identity(task: TaskExecution, payload: BaseModel) -> None:
    payload_execution_id = getattr(payload, "execution_id", None)
    payload_node_id = getattr(payload, "node_id", None)
    payload_attempt = getattr(payload, "attempt", None)
    if (
        payload_execution_id != task.execution_id
        or payload_node_id != task.node_id
        or payload_attempt != task.attempt
    ):
        raise ValueError(
            "task event identity does not match persisted task "
            f"{task.id}: execution_id={payload_execution_id}, "
            f"node_id={payload_node_id}, attempt={payload_attempt}"
        )


def _event_id(message_type: str, aggregate_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"woe:{message_type}:{aggregate_id}")
