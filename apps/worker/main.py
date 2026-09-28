from __future__ import annotations

import asyncio

from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server

from apps.worker.application.executor import WorkerTaskExecutor
from apps.worker.application.ports import WorkerUnitOfWork
from apps.worker.application.registry import HandlerRegistry
from apps.worker.infrastructure.db import (
    SqlAlchemyWorkerUnitOfWork,
    create_engine,
    create_session_factory,
)
from apps.worker.infrastructure.handlers import MockExternalServiceHandler, MockLlmServiceHandler
from apps.worker.infrastructure.kafka import WorkerOutboxPublisher
from apps.worker.interfaces.kafka import TaskCommandConsumer
from packages.config import get_worker_settings
from packages.observability import configure_logging


async def run() -> None:
    settings = get_worker_settings()
    configure_logging(settings.log_level)
    start_http_server(settings.worker_metrics_port)
    engine = create_engine(settings.worker_database_url)
    session_factory = create_session_factory(engine)

    def uow_factory() -> WorkerUnitOfWork:
        return SqlAlchemyWorkerUnitOfWork(session_factory)

    registry = HandlerRegistry()
    registry.register("call_external_service", MockExternalServiceHandler())
    registry.register("llm_service", MockLlmServiceHandler())
    executor = WorkerTaskExecutor(
        uow_factory,
        registry,
        settings.worker_processing_lease_seconds,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    outbox = WorkerOutboxPublisher(session_factory, producer, settings.outbox_poll_interval_seconds)
    consumer = TaskCommandConsumer(settings.kafka_bootstrap_servers, executor)
    try:
        await asyncio.gather(outbox.run(), consumer.run())
    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
