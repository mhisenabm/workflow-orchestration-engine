from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import JsonValue, TypeAdapter, ValidationError

from apps.worker.application.ports import WorkerUnitOfWork
from apps.worker.application.registry import HandlerRegistry
from apps.worker.domain.models import ClaimOutcome, ReceiptStatus, TaskReceipt
from packages.contracts import (
    ExecuteTaskCommand,
    MessageEnvelope,
    TaskCompleted,
    TaskFailed,
    TaskStarted,
    envelope,
)
from packages.contracts.topics import TASK_EVENTS_TOPIC
from packages.observability.metrics import TASK_DURATION, TASK_TRANSITIONS

_JSON_VALUE_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class WorkerTaskExecutor:
    def __init__(
        self,
        uow_factory: Callable[[], WorkerUnitOfWork],
        registry: HandlerRegistry,
        processing_lease_seconds: int,
    ) -> None:
        self._uow_factory = uow_factory
        self._registry = registry
        self._processing_lease_seconds = processing_lease_seconds

    async def process(self, message: MessageEnvelope[ExecuteTaskCommand]) -> bool:
        command = message.payload
        lease_seconds = max(
            self._processing_lease_seconds,
            (command.timeout or 0) + 10,
        )
        now = datetime.now(UTC)
        async with self._uow_factory() as uow:
            claim = await uow.store.claim(command, now + timedelta(seconds=lease_seconds))
            _validate_receipt_identity(command, claim.receipt)
            if claim.outcome is ClaimOutcome.BUSY:
                return False
            if claim.outcome is ClaimOutcome.TERMINAL:
                await self._ensure_terminal_event(uow, claim.receipt)
                await uow.commit()
                return True
            started = envelope(
                "TaskStarted",
                TaskStarted(
                    execution_id=command.execution_id,
                    task_execution_id=command.task_execution_id,
                    node_id=command.node_id,
                    attempt=command.attempt,
                    started_at=now,
                ),
                message_id=_event_id("TaskStarted", command.task_execution_id),
            )
            await uow.store.add_outbox(
                TASK_EVENTS_TOPIC,
                str(command.execution_id),
                started.message_id,
                started.model_dump(mode="json"),
            )
            await uow.commit()

        started_at = perf_counter()
        try:
            handler = self._registry.get(command.handler)
            if command.timeout is None:
                output = await handler.execute(command.config, command.inputs)
            else:
                async with asyncio.timeout(command.timeout):
                    output = await handler.execute(command.config, command.inputs)
        except TimeoutError:
            error: dict[str, Any] = {
                "code": "TASK_TIMEOUT",
                "message": f"Task exceeded its {command.timeout}-second timeout",
            }
            await self._record_failure(command, error)
        except Exception as exc:
            error = {
                "code": "TASK_EXECUTION_FAILED",
                "message": str(exc),
                "exception_type": type(exc).__name__,
            }
            await self._record_failure(command, error)
        else:
            try:
                json_output = _JSON_VALUE_ADAPTER.validate_python(output)
            except ValidationError as exc:
                error = {
                    "code": "INVALID_TASK_OUTPUT",
                    "message": "Task handler returned a value that is not valid JSON",
                    "validation_error": str(exc),
                }
                await self._record_failure(command, error)
            else:
                await self._record_success(command, json_output)
        finally:
            TASK_DURATION.labels(handler=command.handler).observe(perf_counter() - started_at)
        return True

    async def _record_success(self, command: ExecuteTaskCommand, output: Any) -> None:
        now = datetime.now(UTC)
        async with self._uow_factory() as uow:
            receipt = await uow.store.complete(command.task_execution_id, output, now)
            await self._ensure_terminal_event(uow, receipt)
            await uow.commit()
        TASK_TRANSITIONS.labels(service="worker", status="completed", handler=command.handler).inc()

    async def _record_failure(self, command: ExecuteTaskCommand, error: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        async with self._uow_factory() as uow:
            receipt = await uow.store.fail(command.task_execution_id, error, now)
            await self._ensure_terminal_event(uow, receipt)
            await uow.commit()
        TASK_TRANSITIONS.labels(service="worker", status="failed", handler=command.handler).inc()

    async def _ensure_terminal_event(self, uow: WorkerUnitOfWork, receipt: TaskReceipt) -> None:
        payload: TaskCompleted | TaskFailed
        if receipt.status is ReceiptStatus.COMPLETED:
            payload = TaskCompleted(
                execution_id=receipt.execution_id,
                task_execution_id=receipt.task_execution_id,
                node_id=receipt.node_id,
                attempt=receipt.attempt,
                output=receipt.output,
                completed_at=receipt.completed_at or datetime.now(UTC),
            )
            message_type = "TaskCompleted"
        elif receipt.status is ReceiptStatus.FAILED:
            payload = TaskFailed(
                execution_id=receipt.execution_id,
                task_execution_id=receipt.task_execution_id,
                node_id=receipt.node_id,
                attempt=receipt.attempt,
                error=receipt.error
                or {
                    "code": "TASK_EXECUTION_FAILED",
                    "message": "Task failed without error details",
                },
                failed_at=receipt.completed_at or datetime.now(UTC),
            )
            message_type = "TaskFailed"
        else:
            return
        event = envelope(
            message_type,
            payload,
            message_id=_event_id(message_type, receipt.task_execution_id),
        )
        await uow.store.add_outbox(
            TASK_EVENTS_TOPIC,
            str(receipt.execution_id),
            event.message_id,
            event.model_dump(mode="json"),
        )


def _validate_receipt_identity(command: ExecuteTaskCommand, receipt: TaskReceipt) -> None:
    if (
        receipt.execution_id != command.execution_id
        or receipt.node_id != command.node_id
        or receipt.handler != command.handler
        or receipt.attempt != command.attempt
    ):
        raise ValueError(
            "task command identity does not match the existing worker receipt "
            f"{command.task_execution_id}"
        )


def _event_id(message_type: str, task_execution_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"woe:{message_type}:{task_execution_id}")
