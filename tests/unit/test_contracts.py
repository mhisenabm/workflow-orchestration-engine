from uuid import uuid4

import pytest

from packages.contracts import ExecuteTaskCommand, envelope, parse_envelope


def test_contract_round_trip() -> None:
    payload = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="node",
        handler="llm_service",
        config={"prompt": "hello"},
        attempt=1,
    )
    message = envelope("ExecuteTaskCommand", payload)
    parsed = parse_envelope(message.model_dump_json())
    assert parsed.message_id == message.message_id
    assert isinstance(parsed.payload, ExecuteTaskCommand)
    assert parsed.payload == payload


def test_envelope_rejects_mismatched_message_type_and_payload() -> None:
    payload = ExecuteTaskCommand(
        execution_id=uuid4(),
        task_execution_id=uuid4(),
        node_id="test",
        handler="llm_service",
        config={"prompt": "hello"},
        attempt=1,
    )
    with pytest.raises(TypeError, match="requires payload WorkflowStarted"):
        envelope("WorkflowStarted", payload)
    with pytest.raises(ValueError, match="Unsupported message type"):
        envelope("UnknownMessage", payload)
