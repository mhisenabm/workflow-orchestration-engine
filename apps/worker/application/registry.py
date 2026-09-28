from __future__ import annotations

from apps.worker.application.ports import TaskHandler


class HandlerNotFoundError(LookupError):
    pass


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, name: str, handler: TaskHandler) -> None:
        if name in self._handlers:
            raise ValueError(f"handler '{name}' is already registered")
        self._handlers[name] = handler

    def get(self, name: str) -> TaskHandler:
        try:
            return self._handlers[name]
        except KeyError as exc:
            raise HandlerNotFoundError(f"handler '{name}' is not registered") from exc
