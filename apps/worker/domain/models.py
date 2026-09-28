from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class ReceiptStatus(StrEnum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ClaimOutcome(StrEnum):
    EXECUTE = "execute"
    TERMINAL = "terminal"
    BUSY = "busy"


@dataclass(slots=True)
class TaskReceipt:
    task_execution_id: UUID
    execution_id: UUID
    node_id: str
    handler: str
    attempt: int
    status: ReceiptStatus
    lease_expires_at: datetime
    created_at: datetime
    completed_at: datetime | None = None
    output: Any | None = None
    error: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ClaimResult:
    outcome: ClaimOutcome
    receipt: TaskReceipt
