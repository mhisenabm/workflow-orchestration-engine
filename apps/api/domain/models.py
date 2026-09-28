from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class WorkflowExecutionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WorkflowSnapshot:
    id: UUID
    name: str
    definition: dict[str, Any]
    created_at: datetime


@dataclass(slots=True)
class WorkflowExecution:
    id: UUID
    snapshot_id: UUID
    name: str
    status: WorkflowExecutionStatus
    workflow_input: dict[str, Any]
    created_at: datetime
    triggered_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: Any | None = None
    error: dict[str, Any] | None = None
