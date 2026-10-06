from __future__ import annotations

import unittest

from agentic_runtime.evolution.e1 import (
    E1PolicyError,
    compare_e1_metrics,
    monetary_comparison_eligible,
)
from agentic_runtime.cognitive.contracts import UsageState


class E1CostEvidencePolicyTests(unittest.TestCase):
    def test_unverified_worker_usage_cannot_satisfy_hard_money_gate(self):
        self.assertFalse(monetary_comparison_eligible(
            mode="AUTHORITATIVE_ONLY", value=0.0004,
            provenance="UNVERIFIED_WORKER_USAGE"))

    def test_estimate_remains_telemetry_not_measured_money(self):
        estimate = {"amount": 0.0004, "status": "ESTIMATE",
                    "source": "RATE_CARD", "provenance": "UNVERIFIED_WORKER_USAGE"}
        self.assertEqual(estimate["status"], "ESTIMATE")
        self.assertFalse(monetary_comparison_eligible(
            mode="AUTHORITATIVE_ONLY", value=estimate["amount"],
            provenance=estimate["provenance"]))

    def test_unknown_and_subscription_unpriced_remain_non_numeric(self):
        self.assertEqual(UsageState.UNKNOWN.value, "UNKNOWN")
        self.assertEqual(UsageState.SUBSCRIPTION_UNPRICED.value, "SUBSCRIPTION_UNPRICED")
        for state in (UsageState.UNKNOWN.value, UsageState.SUBSCRIPTION_UNPRICED.value):
            self.assertFalse(monetary_comparison_eligible(
                mode="AUTHORITATIVE_ONLY", value=None, provenance=state))

    def test_campaign_safety_only_compares_quality_and_latency_without_cost(self):
        self.assertTrue(monetary_comparison_eligible(
            mode="CAMPAIGN_SAFETY_ONLY", value=None, provenance="UNKNOWN"))
        policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
            "required_improvement":0.05,"protected_quality_metric":"quality",
            "quality_floor":1,"monetary_evidence_mode":"CAMPAIGN_SAFETY_ONLY"}
        result=compare_e1_metrics(baseline={"latency_ms":100,"quality":1,"cost_units":None},
            candidate={"latency_ms":94,"quality":1,"cost_units":None},policy=policy)
        self.assertTrue(result["eligible"])

    def test_hard_monetary_gate_rejects_unverified_estimate(self):
        policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
            "required_improvement":0.05,"protected_quality_metric":"quality",
            "quality_floor":1,"monetary_evidence_mode":"AUTHORITATIVE_ONLY",
            "maximum_cost_units":1}
        result=compare_e1_metrics(baseline={"latency_ms":100,"quality":1},
            candidate={"latency_ms":90,"quality":1,"cost_units":0.0001,
                "cost_provenance":"UNVERIFIED_WORKER_USAGE"},policy=policy)
        self.assertFalse(result["eligible"])
        self.assertFalse(result["monetary_pass"])

    def test_unknown_monetary_mode_fails_closed(self):
        with self.assertRaises(E1PolicyError):
            monetary_comparison_eligible(mode="IGNORE_ALL", value=None, provenance=None)


if __name__ == "__main__":
    unittest.main()
