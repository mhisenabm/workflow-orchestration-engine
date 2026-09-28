from __future__ import annotations

from typing import cast

from aiokafka import AIOKafkaConsumer

from apps.engine.application.orchestrator import EngineOrchestrator
from packages.contracts import ExecuteWorkflowCommand, MessageEnvelope, parse_envelope
from packages.contracts.topics import (
    ENGINE_TASK_EVENTS_GROUP,
    ENGINE_WORKFLOW_GROUP,
    TASK_EVENTS_TOPIC,
    WORKFLOW_COMMANDS_TOPIC,
)


class WorkflowCommandConsumer:
    def __init__(self, bootstrap_servers: str, orchestrator: EngineOrchestrator) -> None:
        self._consumer = AIOKafkaConsumer(
            WORKFLOW_COMMANDS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=ENGINE_WORKFLOW_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._orchestrator = orchestrator

    async def run(self) -> None:
        await self._consumer.start()
        try:
            async for record in self._consumer:
                message = parse_envelope(record.value)
                if not isinstance(message.payload, ExecuteWorkflowCommand):
                    raise ValueError("workflow command topic received wrong message type")
                await self._orchestrator.start_workflow(
                    cast(MessageEnvelope[ExecuteWorkflowCommand], message)
                )
                await self._consumer.commit()
        finally:
            await self._consumer.stop()


class TaskEventConsumer:
    def __init__(self, bootstrap_servers: str, orchestrator: EngineOrchestrator) -> None:
        self._consumer = AIOKafkaConsumer(
            TASK_EVENTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=ENGINE_TASK_EVENTS_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._orchestrator = orchestrator

    async def run(self) -> None:
        await self._consumer.start()
        try:
            async for record in self._consumer:
                message = parse_envelope(record.value)
                await self._orchestrator.process_task_event(message)
                await self._consumer.commit()
        finally:
            await self._consumer.stop()
