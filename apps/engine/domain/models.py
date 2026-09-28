from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class WorkflowRuntimeStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass(slots=True)
class WorkflowRuntime:
    execution_id: UUID
    workflow_snapshot: dict[str, Any]
    workflow_input: dict[str, Any]
    status: WorkflowRuntimeStatus
    started_at: datetime
    completed_at: datetime | None = None
    result: Any | None = None
    error: dict[str, Any] | None = None


@dataclass(slots=True)
class TaskExecution:
    id: UUID
    execution_id: UUID
    node_id: str
    handler: str
    status: TaskStatus
    attempt: int
    config: dict[str, Any]
    inputs: dict[str, Any]
    retry_max_attempts: int
    retry_delay_seconds: float
    timeout: int | None
    available_at: datetime
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: dict[str, Any] | None = None
