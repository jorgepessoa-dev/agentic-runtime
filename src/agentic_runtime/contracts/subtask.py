from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class SubtaskRequest:
    parent_task_id: str
    parent_attempt_id: str
    depth: int
    goal: str
    requested_capabilities: tuple[str, ...]
    input_refs: tuple[str, ...]
    requested_budget: float


@dataclass(frozen=True)
class SubtaskAuthorization:
    approved: bool
    child_task_id: str | None
    reason: str
    reserved_budget: float
    constraints: Mapping[str, object] = field(default_factory=dict)


@dataclass
class _ParentAllocation:
    remaining_budget: float
    child_count: int = 0


class LocalSubtaskAuthorizer:
    """Local authorization gate. It creates no worker or child process."""

    def __init__(self, max_depth: int = 1, max_children_per_task: int = 2,
                 max_total_children: int = 6) -> None:
        self.max_depth = max_depth
        self.max_children_per_task = max_children_per_task
        self.max_total_children = max_total_children
        self._parents: dict[str, _ParentAllocation] = {}
        self._total_children = 0
        self._next_child = 1

    def register_parent(self, task_id: str, budget: float) -> None:
        if budget < 0:
            raise ValueError("budget must be non-negative")
        self._parents[task_id] = _ParentAllocation(budget)

    def request_subtask(self, request: SubtaskRequest) -> SubtaskAuthorization:
        parent = self._parents.get(request.parent_task_id)
        limits = {"max_depth": self.max_depth, "max_children_per_task": self.max_children_per_task,
                  "max_total_children": self.max_total_children}
        if parent is None:
            return SubtaskAuthorization(False, None, "unknown parent task", 0, limits)
        if request.depth >= self.max_depth:
            return SubtaskAuthorization(False, None, "maximum delegation depth reached", 0, limits)
        if parent.child_count >= self.max_children_per_task:
            return SubtaskAuthorization(False, None, "maximum children for parent reached", 0, limits)
        if self._total_children >= self.max_total_children:
            return SubtaskAuthorization(False, None, "campaign child limit reached", 0, limits)
        if request.requested_budget <= 0 or request.requested_budget > parent.remaining_budget:
            return SubtaskAuthorization(False, None, "requested budget exceeds remaining parent allocation", 0, limits)
        parent.remaining_budget -= request.requested_budget
        parent.child_count += 1
        self._total_children += 1
        child_id = f"child-{self._next_child:04d}"
        self._next_child += 1
        return SubtaskAuthorization(True, child_id, "authorized within parent allocation", request.requested_budget, limits)
