from __future__ import annotations

import re
from collections import deque
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

NODE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
TEMPLATE_RE = re.compile(r"{{\s*([A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*)\s*}}")
SUPPORTED_HANDLERS = {"input", "call_external_service", "llm_service", "output"}


class ExtensibleModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class RetryPolicy(ExtensibleModel):
    max_attempts: int = Field(default=1, ge=1)
    delay_seconds: float = Field(default=0, ge=0)


class ExternalServiceConfig(ExtensibleModel):
    url: str = Field(min_length=1)


class LlmServiceConfig(ExtensibleModel):
    prompt: str = Field(min_length=1)


class WorkflowNode(ExtensibleModel):
    id: str
    handler: str
    dependencies: list[str] = Field(default_factory=list)
    config: dict[str, JsonValue] = Field(default_factory=dict)
    inputs: dict[str, JsonValue] = Field(default_factory=dict)
    retry: RetryPolicy | None = None
    timeout: int | None = Field(default=None, gt=0)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_node(self) -> WorkflowNode:
        if not NODE_ID_RE.fullmatch(self.id):
            raise ValueError(
                "node id must start with a letter and contain only letters, numbers, '_' or '-'"
            )
        if self.handler not in SUPPORTED_HANDLERS:
            raise ValueError(f"unsupported handler: {self.handler}")
        if len(self.dependencies) != len(set(self.dependencies)):
            raise ValueError(f"node '{self.id}' contains duplicate dependencies")
        if self.id in self.dependencies:
            raise ValueError(f"node '{self.id}' cannot depend on itself")
        if self.handler == "call_external_service":
            ExternalServiceConfig.model_validate(self.config)
        elif self.handler == "llm_service":
            LlmServiceConfig.model_validate(self.config)
        return self


class WorkflowDag(ExtensibleModel):
    nodes: Annotated[list[WorkflowNode], Field(min_length=2)]


class WorkflowDefinition(ExtensibleModel):
    name: str = Field(min_length=1, max_length=255)
    input: dict[str, JsonValue] = Field(default_factory=dict)
    dag: WorkflowDag

    @model_validator(mode="after")
    def validate_dag(self) -> WorkflowDefinition:
        nodes = self.dag.nodes
        by_id = {node.id: node for node in nodes}
        if len(by_id) != len(nodes):
            duplicates = sorted(
                {node.id for node in nodes if sum(n.id == node.id for n in nodes) > 1}
            )
            raise ValueError(f"duplicate node ids: {', '.join(duplicates)}")

        input_nodes = [node for node in nodes if node.handler == "input"]
        output_nodes = [node for node in nodes if node.handler == "output"]
        if len(input_nodes) != 1 or input_nodes[0].id != "input":
            raise ValueError("workflow must contain exactly one input handler with id 'input'")
        if len(output_nodes) != 1 or output_nodes[0].id != "output":
            raise ValueError("workflow must contain exactly one output handler with id 'output'")
        if input_nodes[0].dependencies:
            raise ValueError("input node cannot have dependencies")
        if not output_nodes[0].dependencies:
            raise ValueError("output node must have at least one dependency")

        for node in nodes:
            missing = [dep for dep in node.dependencies if dep not in by_id]
            if missing:
                raise ValueError(
                    f"node '{node.id}' references missing dependencies: "
                    f"{', '.join(sorted(missing))}"
                )

        children: dict[str, list[str]] = {node.id: [] for node in nodes}
        indegree: dict[str, int] = {node.id: len(node.dependencies) for node in nodes}
        for node in nodes:
            for dependency in node.dependencies:
                children[dependency].append(node.id)

        queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
        visited: list[str] = []
        while queue:
            node_id = queue.popleft()
            visited.append(node_id)
            for child in children[node_id]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if len(visited) != len(nodes):
            raise ValueError("workflow DAG contains a cycle")

        reachable_from_input = _reachable("input", children)
        if reachable_from_input != set(by_id):
            unreachable = sorted(set(by_id) - reachable_from_input)
            raise ValueError(f"nodes are not reachable from input: {', '.join(unreachable)}")

        reverse_children: dict[str, list[str]] = {
            node.id: list(node.dependencies) for node in nodes
        }
        reaches_output = _reachable("output", reverse_children)
        if reaches_output != set(by_id):
            disconnected = sorted(set(by_id) - reaches_output)
            raise ValueError(f"nodes do not lead to output: {', '.join(disconnected)}")

        ancestors = _ancestor_map(nodes)
        for node in nodes:
            for location, value in (("config", node.config), ("inputs", node.inputs)):
                for template in _walk_strings(value):
                    _validate_template_string(template, node, by_id, ancestors[node.id], location)
        return self

    def node_map(self) -> dict[str, WorkflowNode]:
        return {node.id: node for node in self.dag.nodes}

    def ancestors_of(self, node_id: str) -> set[str]:
        return _ancestor_map(self.dag.nodes)[node_id]


def _reachable(start: str, edges: dict[str, list[str]]) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(edges[current])
    return seen


def _ancestor_map(nodes: list[WorkflowNode]) -> dict[str, set[str]]:
    by_id = {node.id: node for node in nodes}
    cache: dict[str, set[str]] = {}

    def ancestors(node_id: str) -> set[str]:
        if node_id in cache:
            return cache[node_id]
        result: set[str] = set()
        for dependency in by_id[node_id].dependencies:
            result.add(dependency)
            result.update(ancestors(dependency))
        cache[node_id] = result
        return result

    return {node.id: ancestors(node.id) for node in nodes}


def _walk_strings(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, str):
        found.append(value)
    elif isinstance(value, dict):
        for child in value.values():
            found.extend(_walk_strings(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_strings(child))
    return found


def _validate_template_string(
    value: str,
    node: WorkflowNode,
    by_id: dict[str, WorkflowNode],
    ancestors: set[str],
    location: str,
) -> None:
    matches = list(TEMPLATE_RE.finditer(value))
    stripped = TEMPLATE_RE.sub("", value)
    if "{{" in stripped or "}}" in stripped:
        raise ValueError(f"malformed template in node '{node.id}' {location}: {value}")
    for match in matches:
        reference = match.group(1)
        source_node = reference.split(".", 1)[0]
        if source_node not in by_id:
            raise ValueError(
                f"template in node '{node.id}' references missing node '{source_node}'"
            )
        if source_node not in ancestors:
            raise ValueError(
                f"template in node '{node.id}' references non-ancestor node '{source_node}'"
            )
