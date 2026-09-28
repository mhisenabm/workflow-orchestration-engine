from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from packages.contracts import ExecuteTaskCommand, envelope, parse_envelope

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_kafka_contract_round_trip() -> None:
    bootstrap = os.getenv("WOE_TEST_KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    group = f"woe-test-{uuid4()}"
    consumer = AIOKafkaConsumer(
        "woe.test.contracts",
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await consumer.start()
    await producer.start()
    try:
        # Wait for Kafka group assignment before publishing. With
        # auto_offset_reset="latest", publishing before assignment can make this
        # test race and miss the record it created.
        for _ in range(100):
            if consumer.assignment():
                break
            await consumer.getmany(timeout_ms=100)
        assert consumer.assignment(), "Kafka consumer was not assigned a partition"

        payload = ExecuteTaskCommand(
            execution_id=uuid4(),
            task_execution_id=uuid4(),
            node_id="test",
            handler="llm_service",
            config={"prompt": "hello"},
            attempt=1,
        )
        message = envelope("ExecuteTaskCommand", payload)
        await producer.send_and_wait(
            "woe.test.contracts",
            key=str(payload.task_execution_id).encode(),
            value=message.model_dump_json().encode(),
        )
        record = await asyncio.wait_for(consumer.getone(), timeout=10)
        parsed = parse_envelope(record.value)
        assert parsed.message_id == message.message_id
        assert parsed.payload == payload
    finally:
        await producer.stop()
        await consumer.stop()
