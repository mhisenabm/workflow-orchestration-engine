from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.infrastructure.db import ApiOutboxRow
from packages.observability.metrics import OUTBOX_BACKLOG


class ApiOutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: AIOKafkaProducer,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._poll_interval = poll_interval

    async def run(self) -> None:
        while True:
            published = await self.publish_batch()
            if published == 0:
                await asyncio.sleep(self._poll_interval)

    async def publish_batch(self, limit: int = 100) -> int:
        async with self._session_factory() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(ApiOutboxRow)
                        .where(ApiOutboxRow.published_at.is_(None))
                        .order_by(ApiOutboxRow.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                await self._producer.send_and_wait(
                    row.topic,
                    key=row.message_key.encode(),
                    value=_json_bytes(row.payload),
                )
                row.published_at = datetime.now(UTC)
            count = await session.scalar(
                select(func.count())
                .select_from(ApiOutboxRow)
                .where(ApiOutboxRow.published_at.is_(None))
            )
            OUTBOX_BACKLOG.labels(service="api").set(count or 0)
            return len(rows)


def _json_bytes(payload: dict[str, object]) -> bytes:
    import json

    return json.dumps(payload, separators=(",", ":"), default=str).encode()
