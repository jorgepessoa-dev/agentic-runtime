from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from agentic_runtime.coordinator.state import TaskState, require_transition
from agentic_runtime.contracts.plan import PlanProposal, ProposedTask, validate_plan
from agentic_runtime.evolution.controller import pareto_dominates
from agentic_runtime.contracts.output import OutputContractError, validate_output


class TaskStateTests(unittest.TestCase):
    def test_valid_result_lifecycle_and_invalid_shortcut(self) -> None:
        require_transition(TaskState.RUNNING, TaskState.RESULT_COMMITTED)
        require_transition(TaskState.RESULT_COMMITTED, TaskState.VERIFIED)
        require_transition(TaskState.VERIFIED, TaskState.ACCEPTED)
        with self.assertRaisesRegex(ValueError, "invalid task state"):
            require_transition(TaskState.RUNNING, TaskState.ACCEPTED)

    def test_expired_worker_may_only_be_requeued(self) -> None:
        require_transition(TaskState.LEASED, TaskState.RETRY_PENDING)
        with self.assertRaises(ValueError):
            require_transition(TaskState.ACCEPTED, TaskState.RETRY_PENDING)


class PlanProposalTests(unittest.TestCase):
    def proposal(self, tasks: tuple[ProposedTask, ...]) -> PlanProposal:
        return PlanProposal("p1", "g1", tasks, None, {"max_cost": 5}, "planner")

    def test_plan_is_structural_proposal_not_authoritative_state(self) -> None:
        validate_plan(self.proposal((ProposedTask("a", "transform", budget={"max_cost": 2}),
                                     ProposedTask("b", "verify", depends_on=("a",), budget={"max_cost": 2}))))

    def test_plan_cycles_and_budget_overrun_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "cycle"):
            validate_plan(self.proposal((ProposedTask("a", "x", depends_on=("b",)),
                                         ProposedTask("b", "y", depends_on=("a",)))))
        with self.assertRaisesRegex(ValueError, "exceed"):
            validate_plan(self.proposal((ProposedTask("a", "x", budget={"max_cost": 6}),)))


class ParetoEvaluationTests(unittest.TestCase):
    def test_tradeoff_does_not_claim_universal_winner(self) -> None:
        directions = {"verified_quality": "MAX", "cost": "MIN"}
        self.assertFalse(pareto_dominates({"verified_quality": 0.9, "cost": 4},
                                          {"verified_quality": 0.8, "cost": 3}, directions))
        self.assertTrue(pareto_dominates({"verified_quality": 0.9, "cost": 3},
                                         {"verified_quality": 0.8, "cost": 4}, directions))


class OutputContractTests(unittest.TestCase):
    def test_json_schema_subset_checks_required_types_and_extra_fields(self) -> None:
        contract = {"schema":{"type":"object","properties":{"ok":{"type":"boolean"}},
                              "required":["ok"],"additionalProperties":False}}
        validate_output(b'{"ok":true}', contract)
        with self.assertRaises(OutputContractError):
            validate_output(b'{"ok":"yes"}', contract)
        with self.assertRaises(OutputContractError):
            validate_output(b'{"ok":true,"extra":1}', contract)


if __name__ == "__main__":
    unittest.main()
