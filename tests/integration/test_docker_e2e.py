from __future__ import annotations

import os
import time

import httpx
import pytest

pytestmark = pytest.mark.integration


WORKFLOW = {
    "name": "Parallel API Fetcher With Summary",
    "input": {"requested_by": "test-user"},
    "dag": {
        "nodes": [
            {"id": "input", "handler": "input", "dependencies": []},
            {
                "id": "get_user",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/users/{{ input.requested_by }}"},
            },
            {
                "id": "get_posts",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://localhost:8911/posts"},
            },
            {
                "id": "generate_summary",
                "handler": "llm_service",
                "dependencies": ["get_user", "get_posts"],
                "config": {
                    "prompt": (
                        "Summarize {{ get_user.data.message }} and {{ get_posts.data.message }}"
                    )
                },
            },
            {"id": "output", "handler": "output", "dependencies": ["generate_summary"]},
        ]
    },
}


def test_complete_workflow() -> None:
    base_url = os.getenv("WOE_TEST_API_URL", "http://localhost:8000")
    with httpx.Client(base_url=base_url, timeout=10) as client:
        submitted = client.post("/workflow", json=WORKFLOW)
        submitted.raise_for_status()
        execution_id = submitted.json()["execution_id"]
        triggered = client.post(f"/workflow/trigger/{execution_id}")
        assert triggered.status_code == 202

        deadline = time.monotonic() + 30
        status = None
        while time.monotonic() < deadline:
            response = client.get(f"/workflows/{execution_id}")
            response.raise_for_status()
            status = response.json()["status"]
            if status in {"completed", "failed"}:
                break
            time.sleep(0.25)
        assert status == "completed"
        result = client.get(f"/workflows/{execution_id}/results")
        result.raise_for_status()
        assert result.json()["result"]["generate_summary"].startswith("Mock LLM response")
