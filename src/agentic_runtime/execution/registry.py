from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from agentic_runtime.contracts.execution import AdapterCapabilities, ExecutionMode, ExecutorClass


@dataclass(frozen=True)
class ExecutorRegistration:
    executor_id: str
    backend: str
    capabilities: AdapterCapabilities
    configuration_ref: str
    adapter: object


class ExecutorRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, ExecutorRegistration] = {}

    def register(self, registration: ExecutorRegistration) -> None:
        if registration.executor_id in self._entries:
            raise ValueError(f"executor already registered: {registration.executor_id}")
        self._entries[registration.executor_id] = registration

    def get(self, executor_id: str) -> ExecutorRegistration:
        return self._entries[executor_id]

    def eligible(self, capability: str, executor_class: ExecutorClass | None = None) -> tuple[ExecutorRegistration, ...]:
        return tuple(entry for entry in self._entries.values()
                     if capability in entry.capabilities.capabilities
                     and (executor_class is None or entry.capabilities.executor_class == executor_class))

    def describe(self) -> tuple[Mapping[str, object], ...]:
        return tuple({"executor_id": e.executor_id, "backend": e.backend,
                      "executor_class": e.capabilities.executor_class.value,
                      "capabilities": sorted(e.capabilities.capabilities),
                      "models": list(e.capabilities.models),
                      "configuration_ref": e.configuration_ref}
                     for e in self._entries.values())


def validate_mode(mode: ExecutionMode, selected: tuple[ExecutorRegistration, ...],
                  *, independent_reviewer: bool = False,
                  deterministic_validation: bool = False) -> None:
    """Check a small explicit execution plan; this does not select or learn routes."""
    if mode == ExecutionMode.CHEAP and len(selected) != 1:
        raise ValueError("CHEAP requires exactly one executor")
    if mode == ExecutionMode.DIVERSE:
        if len(selected) < 2 or len({x.backend for x in selected}) < 2:
            raise ValueError("DIVERSE requires at least two heterogeneous backends")
    if mode == ExecutionMode.CRITICAL:
        if len(selected) != 1 or not independent_reviewer or not deterministic_validation:
            raise ValueError("CRITICAL requires one producer, an independent reviewer and deterministic validation")
