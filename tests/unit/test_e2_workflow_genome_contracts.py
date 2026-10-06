from __future__ import annotations

import unittest
import inspect

from agentic_runtime.evolution.e2 import (
    E2PolicyError, WorkflowMutation, apply_workflow_mutation, digest,
    validate_e2_mutation_scope,
)


def champion() -> dict:
    return {
        "plan_id": "workflow-generic",
        "goal_id": "evaluation-goal",
        "nodes": [
            {"node_key": "inspect", "task_type": "generic_agent_task",
             "objective": "Inspect immutable input", "required_capabilities": ["generic"]},
            {"node_key": "review", "task_type": "generic_agent_task",
             "objective": "Independently review output", "required_capabilities": ["review"],
             "depends_on": ["inspect"],
             "acceptance_criteria": {"requires_independent_verification": True},
             "verifier_capabilities": ["review"]},
        ],
        "estimated_budget": {},
        "bounds": {"max_depth": 2, "max_children_per_node": 3,
                   "max_descendants": 6, "max_concurrent_descendants": 3,
                   "max_retries_per_node": 2, "max_wall_time_seconds": 120,
                   "max_artifact_bytes": 1_048_576},
    }


def mutation(parent: dict, operations: tuple[dict, ...]) -> WorkflowMutation:
    return WorkflowMutation("mutation-1", "workflow-scope", "genome-v1", digest(parent),"observation-1",
        "Observed redundant sequential review", "Reduce wall time without reducing verification",
        "Parallel work may increase resource use", "suite-v1", "genome-v1", operations)


class E2WorkflowGenomeContractTests(unittest.TestCase):
    def test_evaluation_api_requires_accepted_m9_execution_evidence(self):
        from agentic_runtime.evolution.e2_store import E2EvolutionStore
        parameters=inspect.signature(E2EvolutionStore.record_evaluation).parameters
        for required in ("plan_version_id","recovery_event_id","execution_evidence","artifact_refs"):
            with self.subTest(required=required):
                self.assertIn(required,parameters)
                self.assertIs(inspect.Parameter.empty,parameters[required].default)
        self.assertNotIn("metrics",parameters)

    def test_allows_valid_bounded_node_topology_mutation(self):
        parent = champion()
        candidate = apply_workflow_mutation(parent, mutation(parent, (
            {"op": "replace", "path": "/nodes/1/depends_on", "value": []},
        )))
        self.assertEqual(candidate["nodes"][1]["depends_on"], [])
        self.assertEqual(parent["nodes"][1]["depends_on"], ["inspect"])
        tighter=apply_workflow_mutation(parent,mutation(parent,(
            {"op":"replace","path":"/bounds/max_concurrent_descendants","value":2},)))
        self.assertEqual(tighter["bounds"]["max_concurrent_descendants"],2)
        with self.assertRaisesRegex(E2PolicyError,"bounds may only tighten"):
            apply_workflow_mutation(parent,mutation(parent,(
                {"op":"replace","path":"/bounds/max_concurrent_descendants","value":4},)))

    def test_structural_e2_allowlist_rejects_e3_e4_mixed_and_text_bypass(self):
        parent=champion()
        # A pure dependency-topology change remains a valid E2 operation.
        valid=apply_workflow_mutation(parent,mutation(parent,(
            {"op":"replace","path":"/nodes/1/depends_on","value":[]},)))
        self.assertEqual(valid["nodes"][1]["depends_on"],[])
        cases=(
            # Executable schema is an E3 surface.
            ({"op":"replace","path":"/nodes/0/output_contract","value":{"type":"string"}},),
            # Free-form objective is authoritative task/domain content (E4).
            ({"op":"replace","path":"/nodes/0/objective","value":"Apply a domain business rule"},),
            # Mixed proposals fail atomically when any forbidden surface appears.
            ({"op":"replace","path":"/nodes/1/depends_on","value":[]},
             {"op":"replace","path":"/nodes/0/output_contract","value":{"type":"string"}}),
        )
        for operations in cases:
            forged=WorkflowMutation(**{**mutation(parent,operations).__dict__,
                "rationale":"E2 metadata claims this is safe; metadata is not authority"})
            with self.subTest(operations=operations), self.assertRaises(E2PolicyError):
                apply_workflow_mutation(parent,forged)

    def test_added_topology_node_must_reuse_frozen_task_contract(self):
        parent=champion()
        addition={**parent["nodes"][0],"node_key":"inspect-copy","depends_on":["review"]}
        candidate=apply_workflow_mutation(parent,mutation(parent,(
            {"op":"add","path":"/nodes","value":addition},)))
        self.assertEqual(candidate["nodes"][-1]["node_key"],"inspect-copy")
        domain_node={**addition,"node_key":"new-domain-work","objective":"Apply domain business rules"}
        with self.assertRaisesRegex(E2PolicyError,"reuse a frozen task contract"):
            apply_workflow_mutation(parent,mutation(parent,(
                {"op":"add","path":"/nodes","value":domain_node},)))

    def test_rejects_e0_evaluator_promotion_and_e3_fields(self):
        parent = champion()
        for path in ("/evaluator", "/promotion_policy", "/rollback_target", "/runtime_code"):
            with self.subTest(path=path), self.assertRaises(E2PolicyError):
                apply_workflow_mutation(parent, mutation(parent, (
                    {"op": "add", "path": path, "value": {"changed": True}},
                )))

    def test_rejects_invalid_plan_authority_and_stale_parent(self):
        parent = champion()
        with self.assertRaisesRegex(E2PolicyError, "hash mismatch"):
            apply_workflow_mutation(parent, WorkflowMutation(**{
                **mutation(parent, ()).__dict__, "parent_hash": "0" * 64}))
        with self.assertRaisesRegex(E2PolicyError, "cannot add task or verifier capabilities"):
            apply_workflow_mutation(parent, mutation(parent, (
                {"op": "replace", "path": "/nodes/0/required_capabilities",
                 "value": ["governance"]},
            )))

    def test_rejects_duplicate_or_out_of_scope_paths(self):
        parent = champion()
        with self.assertRaisesRegex(E2PolicyError, "duplicate"):
            apply_workflow_mutation(parent, mutation(parent, (
                {"op": "replace", "path": "/nodes/1/depends_on", "value": []},
                {"op": "replace", "path": "/nodes/1/depends_on", "value": ["inspect"]},
            )))
        for tier, path in (("E3", "/nodes"), ("E2", "/bounds"), ("E2", "/evaluator")):
            with self.subTest(tier=tier, path=path), self.assertRaises(E2PolicyError):
                validate_e2_mutation_scope(tier, path)

    def test_rejects_malformed_json_pointer_and_remove_value(self):
        parent=champion()
        for operation in (
            {"op":"replace","path":"/nodes/0/objective~2","value":"x"},
            {"op":"remove","path":"/nodes/0/objective","value":"x"},
        ):
            with self.subTest(operation=operation), self.assertRaises(E2PolicyError):
                apply_workflow_mutation(parent,mutation(parent,(operation,)))

    def test_rejects_malformed_and_privileged_workflow(self):
        parent = champion()
        with self.assertRaisesRegex(E2PolicyError, "structural workflow-genome allowlist"):
            apply_workflow_mutation(parent, mutation(parent, (
                {"op": "remove", "path": "/nodes/1/node_key"},
            )))
        with self.assertRaisesRegex(E2PolicyError, "unsupported fields"):
            apply_workflow_mutation(parent, mutation(parent, (
                {"op": "replace", "path": "/nodes/0/objective", "value": "x",
                 "evaluator": "candidate"},
            )))

    def test_rejects_unmodeled_node_fields_and_unbounded_mutation_size(self):
        parent = champion()
        with self.assertRaisesRegex(E2PolicyError, "structural workflow-genome allowlist"):
            apply_workflow_mutation(parent, mutation(parent, (
                {"op": "add", "path": "/nodes/0/authority", "value": "promotion"},
            )))
        with self.assertRaisesRegex(E2PolicyError, "operation or payload-size bound"):
            apply_workflow_mutation(parent, mutation(parent, tuple(
                {"op": "replace", "path": f"/nodes/0/objective", "value": f"value-{i}"}
                for i in range(13)
            )))


if __name__ == "__main__":
    unittest.main()
