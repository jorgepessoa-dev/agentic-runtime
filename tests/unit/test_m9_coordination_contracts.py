from __future__ import annotations

import unittest

from agentic_runtime.contracts.coordination import (
    PlanNode, PlanVersionProposal, canonical_plan_hash, ready_node_keys,
    validate_plan_version,
)


def sample() -> PlanVersionProposal:
    return PlanVersionProposal(
        plan_id="plan-1", goal_id="goal-1", created_by="planner",
        nodes=(
            PlanNode("a", "inspect", "Inspect immutable input", ("generic_agent_task",)),
            PlanNode("b", "transform", "Transform evidence", ("generic_agent_task",),
                     depends_on=("a",), dependency_requirements={"a":"ARTIFACT"}),
            PlanNode("c", "review", "Independently review", ("generic_agent_task",),
                     depends_on=("a",), dependency_requirements={"a":"ACCEPTED"}),
            PlanNode("join", "summarize", "Join verified branches", ("generic_agent_task",),
                     depends_on=("b", "c")),
        ), estimated_budget={"calls": 4})


class M9CoordinationContractTests(unittest.TestCase):
    def test_validates_dag_and_returns_stable_topology(self):
        proposal = sample()
        self.assertEqual(validate_plan_version(proposal), ("a", "b", "c", "join"))
        self.assertEqual(canonical_plan_hash(proposal), canonical_plan_hash(proposal))

    def test_readiness_observes_dependency_requirement_type(self):
        proposal = sample()
        self.assertEqual(ready_node_keys(proposal, set()), ("a",))
        self.assertEqual(ready_node_keys(proposal, set(), artifact_nodes={"a"}), ("b",))
        self.assertEqual(ready_node_keys(proposal, {"a"}), ("c",))
        self.assertEqual(ready_node_keys(proposal, {"a"}, artifact_nodes={"a"}), ("b", "c"))
        self.assertEqual(ready_node_keys(proposal, {"a", "b", "c"}), ("join",))
        self.assertEqual(ready_node_keys(proposal, {"a", "b", "c", "join"}), ())

    def test_rejects_cycles_and_unknown_dependencies(self):
        proposal = sample()
        cyclic = PlanVersionProposal(**{**proposal.__dict__, "nodes": (
            PlanNode("a", "x", "x", depends_on=("b",)),
            PlanNode("b", "x", "x", depends_on=("a",)))})
        with self.assertRaisesRegex(ValueError, "cycle"):
            validate_plan_version(cyclic)
        unknown = PlanVersionProposal(**{**proposal.__dict__, "nodes": (
            PlanNode("a", "x", "x", depends_on=("missing",)),)})
        with self.assertRaisesRegex(ValueError, "unknown dependency"):
            validate_plan_version(unknown)

    def test_rejects_malformed_node_contracts_and_unknown_delegation_parent(self):
        base = sample()
        malformed = PlanVersionProposal(**{**base.__dict__, "nodes": (
            PlanNode("a", "", "Objective"),)})
        with self.assertRaisesRegex(ValueError, "task type and objective"):
            validate_plan_version(malformed)
        unknown_parent = PlanVersionProposal(**{**base.__dict__, "nodes": (
            PlanNode("a", "task", "Root", parent_node_key="missing"),)})
        with self.assertRaisesRegex(ValueError, "delegation parent is unknown"):
            validate_plan_version(unknown_parent)

    def test_enforces_delegation_depth_and_children_limits(self):
        base = sample()
        too_deep = PlanVersionProposal(**{**base.__dict__, "nodes": (
            PlanNode("a", "task", "Root"),
            PlanNode("b", "task", "Child", parent_node_key="a"),
            PlanNode("c", "task", "Grandchild", parent_node_key="b")),
            "max_depth": 1})
        with self.assertRaisesRegex(ValueError, "delegation depth"):
            validate_plan_version(too_deep)
        too_many_children = PlanVersionProposal(**{**base.__dict__, "nodes": (
            PlanNode("a", "task", "Root"),
            PlanNode("b", "task", "Child 1", parent_node_key="a"),
            PlanNode("c", "task", "Child 2", parent_node_key="a"),
            PlanNode("d", "task", "Child 3", parent_node_key="a")),
            "max_children_per_node": 2})
        with self.assertRaisesRegex(ValueError, "per-node child limit"):
            validate_plan_version(too_many_children)

    def test_rejects_over_limit_and_forbidden_authority(self):
        proposal = sample()
        over = PlanVersionProposal(**{**proposal.__dict__, "max_descendants": 7})
        with self.assertRaisesRegex(ValueError, "above immutable"):
            validate_plan_version(over)
        privileged = PlanVersionProposal(**{**proposal.__dict__, "nodes": (
            PlanNode("root", "x", "x", required_capabilities=("governance",)),)})
        with self.assertRaisesRegex(ValueError, "privileged"):
            validate_plan_version(privileged)

    def test_rejects_budget_overrun(self):
        base=sample()
        nodes=(PlanNode("a","inspect","Inspect immutable input",budget={"calls":2}),)+base.nodes[1:]
        proposal = PlanVersionProposal(**{**base.__dict__, "nodes":nodes,
                                         "estimated_budget":{"calls":1}})
        with self.assertRaisesRegex(ValueError, "exceed estimated budget"):
            validate_plan_version(proposal)

    def test_remote_fake_worker_delay_is_bounded_and_generic_only(self):
        base=sample()
        generic=PlanVersionProposal(**{**base.__dict__,"nodes":(
            PlanNode("a","generic_agent_task","Bounded deterministic branch",
                budget={"worker_sleep_seconds":2}),),
            "estimated_budget":{"worker_sleep_seconds":2}})
        self.assertEqual(validate_plan_version(generic),("a",))
        for task_type,delay in (("generic_agent_task",6),("cognitive_invocation",2)):
            invalid=PlanVersionProposal(**{**generic.__dict__,"nodes":(
                PlanNode("a",task_type,"Invalid deterministic branch delay",
                    budget={"worker_sleep_seconds":delay}),),
                "estimated_budget":{"worker_sleep_seconds":delay}})
            with self.assertRaisesRegex(ValueError,"bounded deterministic worker delay"):
                validate_plan_version(invalid)

    def test_requires_plan_wide_budget_for_every_node_dimension(self):
        base=sample()
        node=PlanNode("a","inspect","Inspect",budget={"tokens":5})
        proposal=PlanVersionProposal(**{**base.__dict__,"nodes":(node,),"estimated_budget":{}})
        with self.assertRaisesRegex(ValueError,"plan-wide bound"):
            validate_plan_version(proposal)

    def test_plan_fanout_can_queue_more_nodes_than_runtime_concurrency_cap(self):
        nodes=tuple(PlanNode(f"root{i}","inspect",f"Inspect {i}") for i in range(4))
        proposal=PlanVersionProposal(plan_id="wide",goal_id="goal",created_by="planner",
            nodes=nodes,estimated_budget={},max_concurrent_descendants=3)
        self.assertEqual(len(validate_plan_version(proposal)),4)


if __name__ == "__main__":
    unittest.main()
