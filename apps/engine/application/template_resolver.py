from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from packages.contracts.workflow import TEMPLATE_RE


class TemplateResolutionError(ValueError):
    def __init__(self, message: str, template: str | None = None) -> None:
        super().__init__(message)
        self.template = template


class TemplateResolver:
    def resolve(self, value: Any, outputs: dict[str, Any], allowed_nodes: set[str]) -> Any:
        return self._resolve_value(deepcopy(value), outputs, allowed_nodes)

    def _resolve_value(self, value: Any, outputs: dict[str, Any], allowed_nodes: set[str]) -> Any:
        if isinstance(value, dict):
            return {
                key: self._resolve_value(child, outputs, allowed_nodes)
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [self._resolve_value(child, outputs, allowed_nodes) for child in value]
        if isinstance(value, str):
            return self._resolve_string(value, outputs, allowed_nodes)
        return value

    def _resolve_string(self, value: str, outputs: dict[str, Any], allowed_nodes: set[str]) -> Any:
        matches = list(TEMPLATE_RE.finditer(value))
        stripped = TEMPLATE_RE.sub("", value)
        if "{{" in stripped or "}}" in stripped:
            raise TemplateResolutionError("malformed template syntax", value)
        if not matches:
            return value
        if len(matches) == 1 and matches[0].span() == (0, len(value)):
            return deepcopy(self._lookup(matches[0].group(1), outputs, allowed_nodes, value))

        result: list[str] = []
        cursor = 0
        for match in matches:
            result.append(value[cursor : match.start()])
            resolved = self._lookup(match.group(1), outputs, allowed_nodes, value)
            result.append(_to_text(resolved))
            cursor = match.end()
        result.append(value[cursor:])
        return "".join(result)

    def _lookup(
        self,
        reference: str,
        outputs: dict[str, Any],
        allowed_nodes: set[str],
        template: str,
    ) -> Any:
        parts = reference.split(".")
        node_id = parts[0]
        if node_id not in allowed_nodes:
            raise TemplateResolutionError(
                f"node '{node_id}' is not an ancestor of the target node", template
            )
        if node_id not in outputs:
            raise TemplateResolutionError(f"output for node '{node_id}' is unavailable", template)
        current: Any = outputs[node_id]
        for key in parts[1:]:
            if not isinstance(current, dict) or key not in current:
                raise TemplateResolutionError(f"output path '{reference}' was not found", template)
            current = current[key]
        return current


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    return str(value)
