from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ProposedTask:
    proposal_id: str
    task_type: str
    required_capabilities: tuple[str, ...] = ()
    input_refs: tuple[str, ...] = ()
    output_contract: Mapping[str, Any] = field(default_factory=dict)
    budget: Mapping[str, float] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlanProposal:
    plan_id: str
    goal_id: str
    proposed_tasks: tuple[ProposedTask, ...]
    rationale_ref: str | None
    estimated_budget: Mapping[str, float]
    created_by: str


def validate_plan(proposal: PlanProposal) -> None:
    """Validate structural safety only; the coordinator remains authoritative."""
    ids = [task.proposal_id for task in proposal.proposed_tasks]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("plan needs uniquely identified proposed tasks")
    known = set(ids)
    graph: dict[str, tuple[str, ...]] = {}
    for task in proposal.proposed_tasks:
        if not task.task_type or any(parent not in known for parent in task.depends_on):
            raise ValueError("plan contains an empty task type or unknown dependency")
        if task.proposal_id in task.depends_on:
            raise ValueError("task cannot depend on itself")
        graph[task.proposal_id] = task.depends_on
        if any(value < 0 for value in task.budget.values()):
            raise ValueError("task budget dimensions must be non-negative")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError("plan dependency graph contains a cycle")
        if node in visited:
            return
        visiting.add(node)
        for parent in graph[node]:
            visit(parent)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)
    for dimension, maximum in proposal.estimated_budget.items():
        total = sum(float(task.budget.get(dimension, 0)) for task in proposal.proposed_tasks)
        if total > float(maximum):
            raise ValueError(f"proposed tasks exceed estimated plan budget for {dimension}")
