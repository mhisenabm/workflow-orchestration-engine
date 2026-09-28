from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any

Sleep = Callable[[float], Awaitable[None]]


class MockExternalServiceHandler:
    def __init__(
        self,
        sleeper: Sleep = asyncio.sleep,
        delay_factory: Callable[[], float] | None = None,
    ) -> None:
        self._sleeper = sleeper
        self._delay_factory = delay_factory or (lambda: random.uniform(1.0, 2.0))

    async def execute(self, config: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
        url = config.get("url")
        if not isinstance(url, str) or not url:
            raise ValueError("call_external_service requires a non-empty config.url")
        delay = self._delay_factory()
        await self._sleeper(delay)
        return {
            "mock": True,
            "status_code": 200,
            "url": url,
            "data": {
                "message": "Mock external service response",
                "inputs": inputs,
            },
        }


class MockLlmServiceHandler:
    def __init__(
        self,
        sleeper: Sleep = asyncio.sleep,
        delay_seconds: float = 0.25,
    ) -> None:
        self._sleeper = sleeper
        self._delay_seconds = delay_seconds

    async def execute(self, config: dict[str, Any], inputs: dict[str, Any]) -> str:
        prompt = config.get("prompt")
        if not isinstance(prompt, str) or not prompt:
            raise ValueError("llm_service requires a non-empty resolved config.prompt")
        await self._sleeper(self._delay_seconds)
        return f"Mock LLM response for prompt: {prompt}"
