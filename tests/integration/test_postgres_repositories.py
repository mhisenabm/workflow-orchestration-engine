from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from apps.api.application.use_cases import SubmitWorkflow, TriggerWorkflow
from apps.api.infrastructure.db import (
    ApiOutboxRow,
    SqlAlchemyApiUnitOfWork,
    WorkflowExecutionRow,
    WorkflowSnapshotRow,
)
from apps.api.infrastructure.db import (
    Base as ApiBase,
)
from apps.api.infrastructure.db import (
    create_engine as create_api_engine,
)
from apps.api.infrastructure.db import (
    create_session_factory as create_api_sessions,
)
from apps.engine.application.orchestrator import EngineOrchestrator
from apps.engine.infrastructure.db import (
    Base as EngineBase,
)
from apps.engine.infrastructure.db import (
    EngineOutboxRow,
    SqlAlchemyEngineUnitOfWork,
    TaskExecutionRow,
)
from apps.engine.infrastructure.db import (
    create_engine as create_engine_engine,
)
from apps.engine.infrastructure.db import (
    create_session_factory as create_engine_sessions,
)
from apps.worker.application.executor import WorkerTaskExecutor
from apps.worker.application.registry import HandlerRegistry
from apps.worker.infrastructure.db import (
    Base as WorkerBase,
)
from apps.worker.infrastructure.db import (
    SqlAlchemyWorkerUnitOfWork,
    WorkerTaskReceiptRow,
)
from apps.worker.infrastructure.db import (
    create_engine as create_worker_engine,
)
from apps.worker.infrastructure.db import (
    create_session_factory as create_worker_sessions,
)
from packages.contracts import (
    ExecuteTaskCommand,
    ExecuteWorkflowCommand,
    TaskCompleted,
    WorkflowDefinition,
    envelope,
)

pytestmark = pytest.mark.integration


async def reset_database(engine, metadata) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)
        await connection.run_sync(metadata.create_all)


@pytest.mark.asyncio
async def test_api_postgres_submission_and_trigger_deduplication() -> None:
    engine = create_api_engine(
        os.getenv(
            "WOE_TEST_API_DATABASE_URL",
            "postgresql+asyncpg://woe:woe@localhost:5433/woe_api_test",
        )
    )
    try:
        await reset_database(engine, ApiBase.metadata)
        sessions = create_api_sessions(engine)
        workflow = WorkflowDefinition.model_validate(
            {
                "name": "API repository",
                "dag": {
                    "nodes": [
                        {"id": "input", "handler": "input"},
                        {
                            "id": "task",
                            "handler": "llm_service",
                            "dependencies": ["input"],
                            "config": {"prompt": "hello"},
                        },
                        {"id": "output", "handler": "output", "dependencies": ["task"]},
                    ]
                },
            }
        )
        submit = SubmitWorkflow(lambda: SqlAlchemyApiUnitOfWork(sessions))
        trigger = TriggerWorkflow(lambda: SqlAlchemyApiUnitOfWork(sessions))
        # Regression: the snapshot parent must be flushed before inserting the
        # workflow_executions child row, otherwise PostgreSQL raises an FK error.
        execution = await submit.execute(workflow)
        first_trigger = await trigger.execute(execution.id)
        second_trigger = await trigger.execute(execution.id)
        async with sessions() as session:
            snapshot_count = await session.scalar(
                select(func.count()).select_from(WorkflowSnapshotRow)
            )
            execution_count = await session.scalar(
                select(func.count()).select_from(WorkflowExecutionRow)
            )
            outbox_count = await session.scalar(select(func.count()).select_from(ApiOutboxRow))
        assert execution.status.value == "pending"
        assert first_trigger.status.value == "pending"
        assert second_trigger.status.value == "pending"
        assert snapshot_count == 1
        assert execution_count == 1
        assert outbox_count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_engine_postgres_concurrent_fan_in_dispatches_once() -> None:
    engine = create_engine_engine(
        os.getenv(
            "WOE_TEST_ENGINE_DATABASE_URL",
            "postgresql+asyncpg://woe:woe@localhost:5434/woe_engine_test",
        )
    )
    try:
        await reset_database(engine, EngineBase.metadata)
        sessions = create_engine_sessions(engine)
        orchestrator = EngineOrchestrator(lambda: SqlAlchemyEngineUnitOfWork(sessions))
        execution_id = uuid4()
        workflow = {
            "name": "Database race",
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
                ExecuteWorkflowCommand(
                    execution_id=execution_id,
                    workflow_snapshot=workflow,
                    input={},
                ),
            )
        )
        async with sessions() as session:
            rows = (
                (
                    await session.execute(
                        select(TaskExecutionRow).where(
                            TaskExecutionRow.execution_id == execution_id,
                            TaskExecutionRow.node_id.in_(["b", "c"]),
                        )
                    )
                )
                .scalars()
                .all()
            )
        by_node = {row.node_id: row for row in rows}

        same_completion_time = datetime.now(UTC).replace(microsecond=123000)

        async def complete(node_id: str, value: str) -> None:
            row = by_node[node_id]
            await orchestrator.process_task_event(
                envelope(
                    "TaskCompleted",
                    TaskCompleted(
                        execution_id=execution_id,
                        task_execution_id=row.id,
                        node_id=node_id,
                        attempt=1,
                        output={"value": value},
                        completed_at=same_completion_time,
                    ),
                )
            )

        await asyncio.gather(complete("b", "B"), complete("c", "C"))
        async with sessions() as session:
            d_count = await session.scalar(
                select(func.count())
                .select_from(TaskExecutionRow)
                .where(
                    TaskExecutionRow.execution_id == execution_id,
                    TaskExecutionRow.node_id == "d",
                    TaskExecutionRow.attempt == 1,
                )
            )
            outbox_payloads = (
                (await session.execute(select(EngineOutboxRow.payload))).scalars().all()
            )
            d_outbox_count = sum(
                payload.get("message_type") == "ExecuteTaskCommand"
                and payload.get("payload", {}).get("node_id") == "d"
                for payload in outbox_payloads
            )
        assert d_count == 1
        assert d_outbox_count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_worker_postgres_duplicate_task_executes_once() -> None:
    engine = create_worker_engine(
        os.getenv(
            "WOE_TEST_WORKER_DATABASE_URL",
            "postgresql+asyncpg://woe:woe@localhost:5435/woe_worker_test",
        )
    )
    try:
        await reset_database(engine, WorkerBase.metadata)
        sessions = create_worker_sessions(engine)

        class CountingHandler:
            def __init__(self) -> None:
                self.calls = 0
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def execute(self, config, inputs):
                self.calls += 1
                self.started.set()
                await self.release.wait()
                return {"ok": True}

        handler = CountingHandler()
        registry = HandlerRegistry()
        registry.register("count", handler)
        executor = WorkerTaskExecutor(lambda: SqlAlchemyWorkerUnitOfWork(sessions), registry, 60)
        command = ExecuteTaskCommand(
            execution_id=uuid4(),
            task_execution_id=uuid4(),
            node_id="count",
            handler="count",
            attempt=1,
        )
        message = envelope("ExecuteTaskCommand", command)
        first = asyncio.create_task(executor.process(message))
        await handler.started.wait()
        assert await executor.process(message) is False
        handler.release.set()
        assert await first is True
        assert await executor.process(message) is True
        async with sessions() as session:
            receipt = await session.get(WorkerTaskReceiptRow, command.task_execution_id)
        assert handler.calls == 1
        assert receipt is not None
        assert receipt.output == {"ok": True}
    finally:
        await engine.dispose()
