from __future__ import annotations

import asyncio

import pytest

from apps.api.main import _critical_tasks_running


@pytest.mark.asyncio
async def test_readiness_detects_stopped_critical_background_task() -> None:
    running = asyncio.create_task(asyncio.sleep(60))
    stopped = asyncio.create_task(asyncio.sleep(0))
    await stopped
    try:
        assert _critical_tasks_running([running]) is True
        assert _critical_tasks_running([running, stopped]) is False
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
