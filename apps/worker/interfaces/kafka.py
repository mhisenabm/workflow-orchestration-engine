from __future__ import annotations

import asyncio
from typing import cast

from aiokafka import AIOKafkaConsumer

from apps.worker.application.executor import WorkerTaskExecutor
from packages.contracts import ExecuteTaskCommand, MessageEnvelope, parse_envelope
from packages.contracts.topics import TASK_COMMANDS_TOPIC, WORKER_GROUP


class TaskCommandConsumer:
    def __init__(self, bootstrap_servers: str, executor: WorkerTaskExecutor) -> None:
        self._consumer = AIOKafkaConsumer(
            TASK_COMMANDS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=WORKER_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._executor = executor

    async def run(self) -> None:
        await self._consumer.start()
        try:
            async for record in self._consumer:
                message = parse_envelope(record.value)
                if not isinstance(message.payload, ExecuteTaskCommand):
                    raise ValueError("task command topic received wrong message type")
                typed = cast(MessageEnvelope[ExecuteTaskCommand], message)
                while not await self._executor.process(typed):  # noqa: ASYNC110
                    await asyncio.sleep(0.5)
                await self._consumer.commit()
        finally:
            await self._consumer.stop()
