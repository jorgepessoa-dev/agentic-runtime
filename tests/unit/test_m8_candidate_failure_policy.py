import unittest

from agentic_runtime.evolution.e1 import E1PolicyError, validate_frozen_candidate_outcomes


class M8CandidateFailurePolicyTests(unittest.TestCase):
    def test_one_candidate_local_failure_keeps_frozen_two_candidate_comparison_viable(self):
        validate_frozen_candidate_outcomes(
            expected_routes=("deepseek-direct-96", "deepseek-direct-128", "deepseek-direct-160"),
            valid_routes=("deepseek-direct-96", "deepseek-direct-128"),
            failed_routes=("deepseek-direct-160",), minimum_valid=2)

    def test_insufficient_valid_challengers_stops_campaign(self):
        with self.assertRaisesRegex(E1PolicyError, "fewer than the frozen minimum"):
            validate_frozen_candidate_outcomes(
                expected_routes=("r96", "r128", "r160"),
                valid_routes=("r96",), failed_routes=("r128", "r160"), minimum_valid=2)

    def test_unaccounted_or_overlapping_candidate_is_rejected(self):
        with self.assertRaisesRegex(E1PolicyError, "do not reconcile"):
            validate_frozen_candidate_outcomes(expected_routes=("r96", "r128", "r160"),
                valid_routes=("r96", "r128"), failed_routes=(), minimum_valid=2)
        with self.assertRaisesRegex(E1PolicyError, "do not reconcile"):
            validate_frozen_candidate_outcomes(expected_routes=("r96", "r128"),
                valid_routes=("r96",), failed_routes=("r96", "r128"), minimum_valid=1)


if __name__ == "__main__":
    unittest.main()
