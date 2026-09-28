import pytest

from apps.worker.application.registry import HandlerNotFoundError, HandlerRegistry
from apps.worker.infrastructure.handlers import MockExternalServiceHandler, MockLlmServiceHandler


@pytest.mark.asyncio
async def test_external_handler_never_calls_network_and_returns_dummy_json() -> None:
    delays: list[float] = []

    async def sleeper(delay: float) -> None:
        delays.append(delay)

    handler = MockExternalServiceHandler(sleeper=sleeper, delay_factory=lambda: 1.5)
    result = await handler.execute({"url": "http://localhost:8911/example"}, {"x": 1})

    assert delays == [1.5]
    assert result["mock"] is True
    assert result["status_code"] == 200
    assert result["url"] == "http://localhost:8911/example"
    assert result["data"]["inputs"] == {"x": 1}


@pytest.mark.asyncio
async def test_llm_handler_returns_mock_string() -> None:
    async def sleeper(_: float) -> None:
        return None

    handler = MockLlmServiceHandler(sleeper=sleeper)
    result = await handler.execute({"prompt": "Summarize John"}, {})
    assert result == "Mock LLM response for prompt: Summarize John"


def test_registry_resolves_by_handler_name() -> None:
    registry = HandlerRegistry()
    handler = MockExternalServiceHandler()
    registry.register("call_external_service", handler)
    assert registry.get("call_external_service") is handler
    with pytest.raises(HandlerNotFoundError):
        registry.get("unknown")
    with pytest.raises(ValueError, match="already registered"):
        registry.register("call_external_service", handler)
