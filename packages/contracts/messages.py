from __future__ import annotations

from datetime import UTC, datetime
from typing import Generic, Literal, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue

PayloadT = TypeVar("PayloadT")


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MessageEnvelope(BaseModel, Generic[PayloadT]):
    model_config = ConfigDict(extra="forbid")

    message_id: UUID = Field(default_factory=uuid4)
    message_type: str
    version: Literal[1] = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: PayloadT


class ExecuteWorkflowCommand(ContractModel):
    execution_id: UUID
    workflow_snapshot: dict[str, JsonValue]
    input: dict[str, JsonValue] = Field(default_factory=dict)


class ExecuteTaskCommand(ContractModel):
    execution_id: UUID
    task_execution_id: UUID
    node_id: str
    handler: str
    config: dict[str, JsonValue] = Field(default_factory=dict)
    inputs: dict[str, JsonValue] = Field(default_factory=dict)
    attempt: int = Field(ge=1)
    timeout: int | None = Field(default=None, gt=0)


class WorkflowStarted(ContractModel):
    execution_id: UUID
    started_at: datetime


class WorkflowCompleted(ContractModel):
    execution_id: UUID
    result: JsonValue
    completed_at: datetime


class WorkflowFailed(ContractModel):
    execution_id: UUID
    error: dict[str, JsonValue]
    failed_at: datetime


class TaskStarted(ContractModel):
    execution_id: UUID
    task_execution_id: UUID
    node_id: str
    attempt: int = Field(ge=1)
    started_at: datetime


class TaskCompleted(ContractModel):
    execution_id: UUID
    task_execution_id: UUID
    node_id: str
    attempt: int = Field(ge=1)
    output: JsonValue
    completed_at: datetime


class TaskFailed(ContractModel):
    execution_id: UUID
    task_execution_id: UUID
    node_id: str
    attempt: int = Field(ge=1)
    error: dict[str, JsonValue]
    failed_at: datetime


MESSAGE_PAYLOADS: dict[str, type[BaseModel]] = {
    "ExecuteWorkflowCommand": ExecuteWorkflowCommand,
    "ExecuteTaskCommand": ExecuteTaskCommand,
    "WorkflowStarted": WorkflowStarted,
    "WorkflowCompleted": WorkflowCompleted,
    "WorkflowFailed": WorkflowFailed,
    "TaskStarted": TaskStarted,
    "TaskCompleted": TaskCompleted,
    "TaskFailed": TaskFailed,
}


def envelope(
    message_type: str, payload: PayloadT, *, message_id: UUID | None = None
) -> MessageEnvelope[PayloadT]:
    payload_type = MESSAGE_PAYLOADS.get(message_type)
    if payload_type is None:
        raise ValueError(f"Unsupported message type: {message_type}")
    if not isinstance(payload, payload_type):
        raise TypeError(
            f"message type '{message_type}' requires payload {payload_type.__name__}, "
            f"got {type(payload).__name__}"
        )
    return MessageEnvelope[PayloadT](
        message_id=message_id or uuid4(),
        message_type=message_type,
        payload=payload,
    )


def parse_envelope(data: bytes | str) -> MessageEnvelope[BaseModel]:
    raw = MessageEnvelope[dict[str, JsonValue]].model_validate_json(data)
    payload_type = MESSAGE_PAYLOADS.get(raw.message_type)
    if payload_type is None:
        raise ValueError(f"Unsupported message type: {raw.message_type}")
    payload = payload_type.model_validate(raw.payload)
    return MessageEnvelope[BaseModel](
        message_id=raw.message_id,
        message_type=raw.message_type,
        version=raw.version,
        occurred_at=raw.occurred_at,
        payload=payload,
    )
