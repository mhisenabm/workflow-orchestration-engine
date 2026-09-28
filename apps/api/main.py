from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.application.ports import ApiUnitOfWork
from apps.api.application.use_cases import (
    GetWorkflow,
    GetWorkflowResults,
    ProjectWorkflowEvent,
    SubmitWorkflow,
    TriggerWorkflow,
)
from apps.api.infrastructure.db import (
    SqlAlchemyApiUnitOfWork,
    create_engine,
    create_session_factory,
)
from apps.api.infrastructure.kafka import ApiOutboxPublisher
from apps.api.interfaces.http import router
from apps.api.interfaces.kafka import WorkflowEventConsumer
from packages.config import ApiSettings, get_api_settings
from packages.observability import configure_logging


@dataclass(slots=True)
class ApiContainer:
    submit_workflow: SubmitWorkflow
    trigger_workflow: TriggerWorkflow
    get_workflow: GetWorkflow
    get_results: GetWorkflowResults
    projector: ProjectWorkflowEvent
    session_factory: async_sessionmaker[AsyncSession]


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or get_api_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings.api_database_url)
    session_factory = create_session_factory(engine)

    def uow_factory() -> ApiUnitOfWork:
        return SqlAlchemyApiUnitOfWork(session_factory)

    container = ApiContainer(
        submit_workflow=SubmitWorkflow(uow_factory),
        trigger_workflow=TriggerWorkflow(uow_factory),
        get_workflow=GetWorkflow(uow_factory),
        get_results=GetWorkflowResults(uow_factory),
        projector=ProjectWorkflowEvent(uow_factory),
        session_factory=session_factory,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await producer.start()
        outbox = ApiOutboxPublisher(
            session_factory, producer, settings.outbox_poll_interval_seconds
        )
        consumer = WorkflowEventConsumer(settings.kafka_bootstrap_servers, container.projector)
        tasks = [asyncio.create_task(outbox.run()), asyncio.create_task(consumer.run())]
        app.state.background_tasks = tasks
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await producer.stop()
            await engine.dispose()

    app = FastAPI(title="Workflow Orchestration Engine API", version="1.0.0", lifespan=lifespan)
    app.state.container = container
    app.state.db_engine = engine
    app.include_router(router)

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        tasks: list[asyncio.Task[None]] = getattr(app.state, "background_tasks", [])
        if not _critical_tasks_running(tasks):
            raise HTTPException(
                status_code=503, detail="a critical Kafka background task has stopped"
            )
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def _critical_tasks_running(tasks: list[asyncio.Task[None]]) -> bool:
    return all(not task.done() for task in tasks)


app = create_app()
