import pytest
from pydantic import ValidationError

from packages.contracts import WorkflowDefinition


def base_workflow() -> dict:
    return {
        "name": "Parallel API Fetcher",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input", "dependencies": []},
                {
                    "id": "get_user",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "http://localhost:8911/users/{{ input.customer_id }}"},
                },
                {
                    "id": "output",
                    "handler": "output",
                    "dependencies": ["get_user"],
                },
            ]
        },
        "input": {"customer_id": 42},
    }


def test_valid_workflow_and_unknown_fields_are_preserved() -> None:
    payload = base_workflow()
    payload["future_workflow_field"] = {"enabled": True}
    payload["dag"]["nodes"][1]["future_node_field"] = "value"

    workflow = WorkflowDefinition.model_validate(payload)

    dumped = workflow.model_dump(mode="json")
    assert dumped["future_workflow_field"] == {"enabled": True}
    assert dumped["dag"]["nodes"][1]["future_node_field"] == "value"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda p: p["dag"]["nodes"].append(dict(p["dag"]["nodes"][1])), "duplicate"),
        (
            lambda p: p["dag"]["nodes"][1].update({"dependencies": ["missing"]}),
            "missing dependencies",
        ),
        (
            lambda p: p["dag"]["nodes"][1].update({"dependencies": ["get_user"]}),
            "cannot depend on itself",
        ),
        (
            lambda p: p["dag"]["nodes"][1].update({"handler": "unknown"}),
            "unsupported handler",
        ),
    ],
)
def test_invalid_workflows(change, message: str) -> None:
    payload = base_workflow()
    change(payload)
    with pytest.raises(ValidationError, match=message):
        WorkflowDefinition.model_validate(payload)


def test_cycle_is_rejected() -> None:
    payload = base_workflow()
    payload["dag"]["nodes"] = [
        {"id": "input", "handler": "input", "dependencies": []},
        {
            "id": "a",
            "handler": "llm_service",
            "dependencies": ["input", "b"],
            "config": {"prompt": "a"},
        },
        {
            "id": "b",
            "handler": "llm_service",
            "dependencies": ["a"],
            "config": {"prompt": "b"},
        },
        {"id": "output", "handler": "output", "dependencies": ["b"]},
    ]
    with pytest.raises(ValidationError, match="cycle"):
        WorkflowDefinition.model_validate(payload)


def test_non_ancestor_template_is_rejected() -> None:
    payload = {
        "name": "invalid reference",
        "dag": {
            "nodes": [
                {"id": "input", "handler": "input"},
                {
                    "id": "a",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "mock://a"},
                },
                {
                    "id": "b",
                    "handler": "llm_service",
                    "dependencies": ["input"],
                    "config": {"prompt": "{{ a.data }}"},
                },
                {"id": "output", "handler": "output", "dependencies": ["a", "b"]},
            ]
        },
    }
    with pytest.raises(ValidationError, match="non-ancestor"):
        WorkflowDefinition.model_validate(payload)
