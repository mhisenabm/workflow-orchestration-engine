from __future__ import annotations

from aiokafka import AIOKafkaConsumer

from apps.api.application.use_cases import ProjectWorkflowEvent
from packages.contracts import parse_envelope
from packages.contracts.topics import API_WORKFLOW_EVENTS_GROUP, WORKFLOW_EVENTS_TOPIC


class WorkflowEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        projector: ProjectWorkflowEvent,
    ) -> None:
        self._consumer = AIOKafkaConsumer(
            WORKFLOW_EVENTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=API_WORKFLOW_EVENTS_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._projector = projector

    async def run(self) -> None:
        await self._consumer.start()
        try:
            async for record in self._consumer:
                message = parse_envelope(record.value)
                await self._projector.execute(message)
                await self._consumer.commit()
        finally:
            await self._consumer.stop()
