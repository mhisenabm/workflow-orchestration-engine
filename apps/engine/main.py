from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server

from apps.engine.application.orchestrator import EngineOrchestrator
from apps.engine.application.ports import EngineUnitOfWork
from apps.engine.infrastructure.db import (
    SqlAlchemyDueRetryFinder,
    SqlAlchemyEngineUnitOfWork,
    create_engine,
    create_session_factory,
)
from apps.engine.infrastructure.kafka import EngineOutboxPublisher
from apps.engine.interfaces.kafka import TaskEventConsumer, WorkflowCommandConsumer
from packages.config import get_engine_settings
from packages.observability import configure_logging


async def run() -> None:
    settings = get_engine_settings()
    configure_logging(settings.log_level)
    start_http_server(settings.engine_metrics_port)
    engine = create_engine(settings.engine_database_url)
    session_factory = create_session_factory(engine)

    def uow_factory() -> EngineUnitOfWork:
        return SqlAlchemyEngineUnitOfWork(session_factory)

    orchestrator = EngineOrchestrator(uow_factory)
    retry_finder = SqlAlchemyDueRetryFinder(session_factory)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    outbox = EngineOutboxPublisher(session_factory, producer, settings.outbox_poll_interval_seconds)
    workflow_consumer = WorkflowCommandConsumer(settings.kafka_bootstrap_servers, orchestrator)
    task_consumer = TaskEventConsumer(settings.kafka_bootstrap_servers, orchestrator)

    async def retry_loop() -> None:
        while True:
            for task_id in await retry_finder.find_due_retry_ids(datetime.now(UTC)):
                await orchestrator.dispatch_retry(task_id)
            await asyncio.sleep(settings.retry_poll_interval_seconds)

    try:
        await asyncio.gather(
            outbox.run(),
            workflow_consumer.run(),
            task_consumer.run(),
            retry_loop(),
        )
    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
