"""Deterministic contracts for bounded E2 workflow-genome mutations.

This module validates declarative declarative coordination-plan template mutations. It has no
database, provider, promotion, or runtime-code mutation capability.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping, Sequence

from agentic_runtime.contracts.coordination import PlanNode, PlanVersionProposal, validate_plan_version
from agentic_runtime.contracts.serialization import canonical_bytes as _canonical_bytes
from agentic_runtime.contracts.serialization import canonical_hash


class E2PolicyError(ValueError):
    """An E2 proposal violates its frozen scope or workflow contract."""


E2_STRUCTURAL_ALLOWLIST = {
    "append": "/nodes (clone an existing frozen task contract)",
    "remove": "/nodes/{index}",
    "mutable_node_fields": ["depends_on", "dependency_requirements", "parent_node_key",
                             "required_capabilities", "verifier_capabilities", "budget"],
    "mutable_workflow_bounds": ["max_depth", "max_children_per_node", "max_descendants",
                                 "max_concurrent_descendants", "max_retries_per_node",
                                 "max_wall_time_seconds", "max_artifact_bytes"],
    "capability_rule": "existing capabilities may only be removed",
    "budget_rule": "existing workflow limits may only be tightened",
    "immutable_surfaces": ["task_type", "objective", "input_refs", "output_contract",
                            "acceptance_criteria", "domain_schema", "domain_rules",
                            "adapter", "tools", "provider_config", "runtime_code",
                            "evaluator", "promotion", "governance", "constitution"],
}


def e2_structural_allowlist_hash() -> str:
    return digest(E2_STRUCTURAL_ALLOWLIST)


def canonical_json(value: Any) -> bytes:
    return _canonical_bytes(value)


def digest(value: Any) -> str:
    return canonical_hash(value)


@dataclass(frozen=True)
class WorkflowMutation:
    mutation_id: str
    scope_id: str
    parent_genome_id: str
    parent_hash: str
    observation_id: str
    rationale: str
    expected_effect: str
    risk: str
    evaluation_plan_ref: str
    rollback_genome_id: str
    operations: tuple[Mapping[str, Any], ...]


def _plan_from_dict(value: Mapping[str, Any]) -> PlanVersionProposal:
    try:
        allowed_root = {"plan_id", "goal_id", "nodes", "estimated_budget", "bounds"}
        allowed_node = {"node_key", "task_type", "objective", "required_capabilities", "input_refs",
                        "output_contract", "acceptance_criteria", "verifier_capabilities", "depends_on",
                        "dependency_requirements", "budget", "parent_node_key"}
        if set(value) - allowed_root:
            raise ValueError("unexpected workflow fields")
        if any(set(item) - allowed_node for item in value["nodes"]):
            raise ValueError("unexpected workflow node fields")
        allowed_bounds = {"max_depth", "max_children_per_node", "max_descendants",
                          "max_concurrent_descendants", "max_retries_per_node",
                          "max_wall_time_seconds", "max_artifact_bytes"}
        if set(value.get("bounds", {})) - allowed_bounds:
            raise ValueError("unexpected workflow bounds")
        nodes = tuple(PlanNode(
            node_key=str(item["node_key"]), task_type=str(item["task_type"]),
            objective=str(item["objective"]),
            required_capabilities=tuple(item.get("required_capabilities", ())),
            input_refs=tuple(item.get("input_refs", ())),
            output_contract=item.get("output_contract", {}),
            acceptance_criteria=item.get("acceptance_criteria", {}),
            verifier_capabilities=tuple(item.get("verifier_capabilities", ())),
            depends_on=tuple(item.get("depends_on", ())),
            dependency_requirements=item.get("dependency_requirements", {}),
            budget=item.get("budget", {}), parent_node_key=item.get("parent_node_key"),
        ) for item in value["nodes"])
        bounds = value.get("bounds", {})
        return PlanVersionProposal(
            plan_id=str(value["plan_id"]), goal_id=str(value["goal_id"]),
            created_by="e2-validated-proposal", nodes=nodes,
            estimated_budget=value.get("estimated_budget", {}),
            max_depth=int(bounds.get("max_depth", 2)),
            max_children_per_node=int(bounds.get("max_children_per_node", 3)),
            max_descendants=int(bounds.get("max_descendants", 6)),
            max_concurrent_descendants=int(bounds.get("max_concurrent_descendants", 3)),
            max_retries_per_node=int(bounds.get("max_retries_per_node", 2)),
            max_wall_time_seconds=int(bounds.get("max_wall_time_seconds", 120)),
            max_artifact_bytes=int(bounds.get("max_artifact_bytes", 1_048_576)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise E2PolicyError("workflow template is malformed") from exc


def _get(document: Any, parts: Sequence[str]) -> Any:
    current = document
    for part in parts:
        current = current[int(part)] if isinstance(current, list) else current[part]
    return current


def _set(document: Any, parts: Sequence[str], value: Any, *, remove: bool = False) -> None:
    if not parts:
        raise E2PolicyError("root replacement is forbidden; propose a bounded node-level delta")
    parent = _get(document, parts[:-1]) if len(parts) > 1 else document
    key = parts[-1]
    if isinstance(parent, list):
        index = int(key)
        if remove:
            parent.pop(index)
        elif key == "-":
            parent.append(value)
        else:
            parent[index] = value
    elif remove:
        del parent[key]
    else:
        parent[key] = value


def apply_workflow_mutation(parent: Mapping[str, Any], mutation: WorkflowMutation, *,
                            persisted_parent_hash: str | None = None) -> dict[str, Any]:
    """Apply structural E2 deltas; task semantics and executable surfaces stay frozen."""
    if not all((mutation.mutation_id, mutation.scope_id, mutation.parent_genome_id,
                mutation.observation_id,mutation.rationale.strip(), mutation.expected_effect.strip(),
                mutation.risk.strip(), mutation.evaluation_plan_ref,
                mutation.rollback_genome_id)):
        raise E2PolicyError("mutation identity, rationale, effect, risk, evaluation and rollback are required")
    if (persisted_parent_hash if persisted_parent_hash is not None else digest(parent)) != mutation.parent_hash:
        raise E2PolicyError("parent workflow hash mismatch")
    if len(mutation.operations)>12 or len(canonical_json(list(mutation.operations)))>1_048_576:
        raise E2PolicyError("mutation exceeds the frozen operation or payload-size bound")
    if any(len(str(operation.get("path","")))>512 for operation in mutation.operations):
        raise E2PolicyError("mutation path exceeds the frozen path-length bound")
    candidate = json.loads(canonical_json(parent))
    if set(candidate) - {"plan_id", "goal_id", "nodes", "estimated_budget", "bounds"}:
        raise E2PolicyError("workflow template contains unsupported or authority-bearing fields")
    allowed_roots = {"nodes", "bounds"}
    mutable_node_fields = set(E2_STRUCTURAL_ALLOWLIST["mutable_node_fields"])
    mutable_workflow_bounds = set(E2_STRUCTURAL_ALLOWLIST["mutable_workflow_bounds"])
    seen: set[str] = set()
    for operation in mutation.operations:
        if set(operation) - {"op", "path", "value"}:
            raise E2PolicyError("mutation operation has unsupported fields")
        op, path = operation.get("op"), operation.get("path")
        if op not in {"add", "remove", "replace"} or not isinstance(path, str) or not path.startswith("/"):
            raise E2PolicyError("mutation operation must be a supported JSON-pointer operation")
        if (op == "remove" and "value" in operation) or (op in {"add", "replace"} and "value" not in operation):
            raise E2PolicyError("mutation operation has an invalid value field")
        if any("~" in part.replace("~0", "").replace("~1", "") for part in path[1:].split("/")):
            raise E2PolicyError("mutation path contains an invalid JSON-pointer escape")
        parts = [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")]
        if not parts or parts[0] not in allowed_roots:
            raise E2PolicyError("only workflow node topology/contracts may be mutated")
        structural = (parts == ["nodes"] and op == "add") or (len(parts) == 2 and op == "remove")
        structural = structural or (len(parts) == 3 and parts[1].isdigit()
                                    and parts[2] in mutable_node_fields)
        structural = structural or (len(parts) == 2 and parts[0] == "bounds"
                                    and parts[1] in mutable_workflow_bounds and op == "replace")
        if not structural:
            raise E2PolicyError("E2 mutation is outside the structural workflow-genome allowlist")
        if path in seen:
            raise E2PolicyError("duplicate mutation path")
        seen.add(path)
        if op == "add":
            if "value" not in operation:
                raise E2PolicyError("add operation requires a value")
            if len(parts) == 1 and parts[0] == "nodes":
                candidate["nodes"].append(operation["value"])
            else:
                _set(candidate, parts, operation["value"])
        else:
            try:
                _get(candidate, parts)
            except (KeyError, IndexError, ValueError, TypeError) as exc:
                raise E2PolicyError("mutation path does not exist") from exc
            if op == "remove":
                _set(candidate, parts, None, remove=True)
            else:
                _set(candidate, parts, operation["value"])

    # A new topology node may only reuse an already frozen task contract.
    # Free-form objective/schema/tool surfaces cannot be introduced by a candidate.
    parent_nodes = {node["node_key"]: node for node in parent["nodes"]}
    candidate_nodes = {node["node_key"]: node for node in candidate["nodes"]}
    structural_fields = {"node_key", "depends_on", "dependency_requirements", "parent_node_key",
                         "required_capabilities", "verifier_capabilities", "budget"}
    def frozen_task_contract(node: Mapping[str, Any]) -> bytes:
        return canonical_json({key: value for key, value in node.items() if key not in structural_fields})
    for key, before in parent_nodes.items():
        after = candidate_nodes.get(key)
        if after is None:
            continue
        if frozen_task_contract(before) != frozen_task_contract(after):
            raise E2PolicyError("E2 cannot alter frozen task, domain, tool, schema, or execution surfaces")
        for capability_field in ("required_capabilities", "verifier_capabilities"):
            if not set(after.get(capability_field, ())).issubset(set(before.get(capability_field, ()))):
                raise E2PolicyError("E2 cannot add task or verifier capabilities")
        old_budget, new_budget = before.get("budget", {}), after.get("budget", {})
        if set(new_budget) - set(old_budget) or any(
                float(value) > float(old_budget[name]) for name, value in new_budget.items()):
            raise E2PolicyError("E2 workflow budget may only tighten existing limits")
    for key, added in candidate_nodes.items():
        if key in parent_nodes:
            continue
        if not any(frozen_task_contract(added) == frozen_task_contract(existing)
                   for existing in parent_nodes.values()):
            raise E2PolicyError("new E2 topology nodes must reuse a frozen task contract")
    old_bounds, new_bounds = parent.get("bounds", {}), candidate.get("bounds", {})
    if (set(new_bounds) != set(old_bounds)
            or set(new_bounds) - mutable_workflow_bounds
            or any(int(value) > int(old_bounds[name]) for name, value in new_bounds.items())):
        raise E2PolicyError("E2 workflow bounds may only tighten existing limits")
    proposal = _plan_from_dict(candidate)
    try:
        validate_plan_version(proposal)
    except ValueError as exc:
        raise E2PolicyError(f"mutated workflow violates coordination-plan contract: {exc}") from exc
    return candidate


def validate_e2_mutation_scope(tier: str, path: str) -> None:
    """Reject tier/path escalation; E2 does not expand existing E1 authority."""
    if tier != "E2":
        raise E2PolicyError("workflow-genome mutation proposals must target E2 only")
    if path == "/nodes":
        return
    parts=path.split("/")
    mutable=set(E2_STRUCTURAL_ALLOWLIST["mutable_node_fields"])
    workflow_bounds=set(E2_STRUCTURAL_ALLOWLIST["mutable_workflow_bounds"])
    if not ((len(parts)==3 and parts[1].isdigit() and parts[2] in mutable)
            or (len(parts)==3 and parts[1]=="bounds" and parts[2] in workflow_bounds)):
        raise E2PolicyError("E2 mutation path is outside the structural workflow-genome allowlist")
