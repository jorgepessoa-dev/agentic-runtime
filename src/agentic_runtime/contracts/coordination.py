"""Provider-neutral, bounded plan and task-graph contracts for durable task coordination."""
from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
import math
from typing import Any, Mapping


@dataclass(frozen=True)
class PlanNode:
    node_key: str
    task_type: str
    objective: str
    required_capabilities: tuple[str, ...] = ()
    input_refs: tuple[str, ...] = ()
    output_contract: Mapping[str, Any] = field(default_factory=dict)
    acceptance_criteria: Mapping[str, Any] = field(default_factory=dict)
    verifier_capabilities: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    dependency_requirements: Mapping[str, str] = field(default_factory=dict)
    budget: Mapping[str, float] = field(default_factory=dict)
    parent_node_key: str | None = None


@dataclass(frozen=True)
class PlanVersionProposal:
    plan_id: str
    goal_id: str
    created_by: str
    nodes: tuple[PlanNode, ...]
    estimated_budget: Mapping[str, float]
    parent_version: int | None = None
    max_depth: int = 2
    max_children_per_node: int = 3
    max_descendants: int = 6
    max_concurrent_descendants: int = 3
    max_retries_per_node: int = 2
    max_wall_time_seconds: int = 120
    max_artifact_bytes: int = 1_048_576


@dataclass(frozen=True)
class PlanBounds:
    max_depth: int = 2
    max_children_per_node: int = 3
    max_descendants: int = 6
    max_concurrent_descendants: int = 3
    max_retries_per_node: int = 2
    max_wall_time_seconds: int = 120
    max_artifact_bytes: int = 1_048_576
    max_nodes: int = 7
    max_plan_versions: int = 3


def canonical_plan_hash(proposal: PlanVersionProposal) -> str:
    payload = {
        "plan_id": proposal.plan_id, "goal_id": proposal.goal_id,
        "created_by": proposal.created_by, "parent_version": proposal.parent_version,
        "nodes": [
            {"node_key": n.node_key, "task_type": n.task_type, "objective": n.objective,
             "required_capabilities": list(n.required_capabilities), "input_refs": list(n.input_refs),
             "output_contract": n.output_contract, "acceptance_criteria": n.acceptance_criteria,
             "verifier_capabilities": list(n.verifier_capabilities), "depends_on": list(n.depends_on),
             "dependency_requirements": dict(n.dependency_requirements),
             "budget": dict(n.budget), "parent_node_key": n.parent_node_key} for n in proposal.nodes
        ], "estimated_budget": dict(proposal.estimated_budget),
        "bounds": {k: getattr(proposal, k) for k in (
            "max_depth", "max_children_per_node", "max_descendants",
            "max_concurrent_descendants", "max_retries_per_node",
            "max_wall_time_seconds", "max_artifact_bytes")},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(raw.encode("utf-8")).hexdigest()


def validate_plan_version(proposal: PlanVersionProposal,
                          bounds: PlanBounds = PlanBounds()) -> tuple[str, ...]:
    """Validate a proposal structurally and enforce hard runtime ceilings."""
    if not proposal.plan_id or not proposal.goal_id or not proposal.created_by:
        raise ValueError("plan identity, goal, and proposer are required")
    if not proposal.nodes or len(proposal.nodes) > bounds.max_nodes:
        raise ValueError("plan node count is empty or exceeds the hard limit")
    keys = [n.node_key for n in proposal.nodes]
    if any(not key for key in keys) or len(keys) != len(set(keys)):
        raise ValueError("plan node keys must be non-empty and unique")
    if (proposal.max_depth > bounds.max_depth or
        proposal.max_children_per_node > bounds.max_children_per_node or
        proposal.max_descendants > bounds.max_descendants or
        proposal.max_concurrent_descendants > bounds.max_concurrent_descendants or
        proposal.max_retries_per_node > bounds.max_retries_per_node or
        proposal.max_wall_time_seconds > bounds.max_wall_time_seconds or
        proposal.max_artifact_bytes > bounds.max_artifact_bytes):
        raise ValueError("plan requests limits above immutable runtime bounds")
    if min(proposal.max_depth, proposal.max_children_per_node,
           proposal.max_descendants, proposal.max_concurrent_descendants,
           proposal.max_retries_per_node, proposal.max_wall_time_seconds,
           proposal.max_artifact_bytes) < 0:
        raise ValueError("plan bounds must be non-negative")
    if proposal.max_concurrent_descendants < 1 or proposal.max_wall_time_seconds < 1 \
            or proposal.max_artifact_bytes < 1:
        raise ValueError("concurrency, wall-time, and artifact bounds must be positive")
    known = set(keys)
    graph: dict[str, tuple[str, ...]] = {}
    for node in proposal.nodes:
        if not node.task_type.strip() or not node.objective.strip():
            raise ValueError("each node requires a task type and objective")
        if len(node.depends_on) != len(set(node.depends_on)):
            raise ValueError("duplicate dependency edge")
        if any(dep not in known for dep in node.depends_on):
            raise ValueError("plan contains an unknown dependency")
        if set(node.dependency_requirements) - set(node.depends_on):
            raise ValueError("dependency requirement refers to a non-edge")
        if any(value not in {"ACCEPTED", "ARTIFACT", "ORDER_ONLY"}
               for value in node.dependency_requirements.values()):
            raise ValueError("unknown dependency requirement")
        if node.node_key in node.depends_on:
            raise ValueError("task cannot depend on itself")
        if not isinstance(node.output_contract, Mapping) or not isinstance(node.acceptance_criteria, Mapping):
            raise ValueError("output and acceptance contracts must be objects")
        if any(not math.isfinite(float(value)) or float(value) < 0
               for value in node.budget.values()):
            raise ValueError("node budget dimensions must be finite and non-negative")
        if "worker_sleep_seconds" in node.budget and (node.task_type!="generic_agent_task"
                or float(node.budget["worker_sleep_seconds"])>5):
            raise ValueError("bounded deterministic worker delay requires a generic task")
        attempts=int(node.budget.get("max_attempts", proposal.max_retries_per_node + 1))
        if attempts < 1 or attempts > proposal.max_retries_per_node + 1:
            raise ValueError("node retry budget exceeds the plan retry limit")
        if set(node.required_capabilities) & {"governance", "database_admin", "evaluator_authority", "root"}:
            raise ValueError("planner cannot request privileged runtime authority")
        graph[node.node_key] = node.depends_on
        if node.acceptance_criteria.get("requires_independent_verification") \
                and not node.verifier_capabilities:
            raise ValueError("independent verification requires verifier capabilities")
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            raise ValueError("plan dependency graph contains a cycle")
        if key in visited:
            return
        visiting.add(key)
        for dependency in graph[key]:
            visit(dependency)
        visiting.remove(key)
        visited.add(key)

    for key in graph:
        visit(key)
    roots = [key for key, dependencies in graph.items() if not dependencies]
    if not roots:
        raise ValueError("plan must contain a root node")
    parents={node.node_key:node.parent_node_key for node in proposal.nodes}
    if any(parent is not None and parent not in known for parent in parents.values()):
        raise ValueError("delegation parent is unknown")
    if any(parent==key for key,parent in parents.items()):
        raise ValueError("task cannot delegate to itself")
    delegation_children: dict[str, list[str]]={key:[] for key in graph}
    for child,parent in parents.items():
        if parent is not None:
            delegation_children[parent].append(child)
    if max(map(len,delegation_children.values()),default=0) > proposal.max_children_per_node:
        raise ValueError("plan exceeds per-node child limit")
    delegation_levels: dict[str,int]={}
    def delegation_depth(key: str, visiting: set[str] | None = None) -> int:
        if key in delegation_levels:
            return delegation_levels[key]
        visiting=visiting or set()
        if key in visiting:
            raise ValueError("delegation hierarchy contains a cycle")
        visiting.add(key)
        parent=parents[key]
        depth=0 if parent is None else delegation_depth(parent,visiting)+1
        visiting.remove(key)
        delegation_levels[key]=depth
        return depth
    for key in graph:
        delegation_depth(key)
    if max(delegation_levels.values(), default=0) > proposal.max_depth:
        raise ValueError("plan exceeds delegation depth limit")
    delegation_roots=sum(parent is None for parent in parents.values())
    if len(graph) - delegation_roots > proposal.max_descendants:
        raise ValueError("plan exceeds descendant limit")
    budget_dimensions={dimension for node in proposal.nodes for dimension in node.budget}
    if not budget_dimensions.issubset(set(proposal.estimated_budget)):
        raise ValueError("every node budget dimension requires a plan-wide bound")
    for dimension, maximum in proposal.estimated_budget.items():
        if not math.isfinite(float(maximum)) or float(maximum) < 0:
            raise ValueError("estimated budget must be finite and non-negative")
        total = sum(float(node.budget.get(dimension, 0)) for node in proposal.nodes)
        if total > float(maximum):
            raise ValueError(f"plan nodes exceed estimated budget for {dimension}")
    return tuple(keys)


def ready_node_keys(proposal: PlanVersionProposal,
                    accepted_nodes: set[str],
                    artifact_nodes: set[str] | None = None,
                    terminal_nodes: set[str] | None = None) -> tuple[str, ...]:
    """Return deterministic readiness using accepted/artifact/terminal evidence."""
    validate_plan_version(proposal)
    artifact_nodes = artifact_nodes or set()
    terminal_nodes = terminal_nodes or set()
    ready = []
    for node in proposal.nodes:
        if node.node_key in accepted_nodes or node.node_key in artifact_nodes or node.node_key in terminal_nodes:
            continue
        satisfied = True
        for dependency in node.depends_on:
            requirement = node.dependency_requirements.get(dependency, "ACCEPTED")
            if requirement == "ACCEPTED" and dependency not in accepted_nodes:
                satisfied = False
            elif requirement == "ARTIFACT" and dependency not in artifact_nodes:
                satisfied = False
            elif requirement == "ORDER_ONLY" and dependency not in terminal_nodes:
                satisfied = False
        if satisfied:
            ready.append(node.node_key)
    return tuple(sorted(ready))
