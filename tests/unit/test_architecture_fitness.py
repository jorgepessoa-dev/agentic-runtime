from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

from agentic_runtime.coordinator.state import TRANSITIONS, TaskState


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "src" / "agentic_runtime"


def _module_graph() -> dict[str, set[str]]:
    modules = {
        path.relative_to(SOURCE).with_suffix("").as_posix().replace("/", "."): path
        for path in SOURCE.rglob("*.py")
    }
    graph = {module: set() for module in modules}
    for module, path in modules.items():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported = [item.name for item in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported = [node.module]
            else:
                continue
            for name in imported:
                name = name.removeprefix("agentic_runtime.")
                if name in modules:
                    graph[module].add(name)
    return graph


def _sql_transitions(path: Path) -> dict[str, set[str]]:
    sql = path.read_text(encoding="utf-8")
    block = sql[sql.index("ok := CASE OLD.status"):sql.index("ELSE false END", sql.index("ok := CASE OLD.status"))]
    return {
        source: set(re.findall(r"'([A-Z_]+)'", targets))
        for source, targets in re.findall(
            r"WHEN '([A-Z_]+)' THEN NEW\.status IN \(([^)]*)\)", block
        )
    }


class ArchitectureFitnessTests(unittest.TestCase):
    def test_package_import_graph_is_acyclic(self) -> None:
        graph = _module_graph()
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(module: str) -> None:
            if module in visiting:
                self.fail(f"package import cycle includes {module}")
            if module in visited:
                return
            visiting.add(module)
            for dependency in graph[module]:
                visit(dependency)
            visiting.remove(module)
            visited.add(module)

        for module in graph:
            visit(module)

    def test_database_task_transition_guard_matches_python_authority(self) -> None:
        expected = {
            state.value: {target.value for target in targets}
            for state, targets in TRANSITIONS.items()
        }
        actual = _sql_transitions(ROOT / "migrations/runtime/0005_m4b_expired_running_retry.sql")
        # The database has one additional, lease-fenced recovery edge which is
        # deliberately conditional on an EXPIRED lease within the SQL guard.
        actual.setdefault("RUNNING", set()).difference_update({"RETRY_PENDING"})
        self.assertEqual(actual, expected)
        self.assertEqual(set(expected), set(actual))

    def test_m8_campaign_cleanup_is_outside_coordinator_core(self) -> None:
        core = (SOURCE / "coordinator/service.py").read_text(encoding="utf-8")
        self.assertNotIn("cancel_m8_test_campaign", core)
        private_admin = SOURCE / "admin/m8.py"
        if private_admin.exists():
            self.assertIn("def cancel_test_campaign", private_admin.read_text(encoding="utf-8"))
        else:
            # Public exports deliberately omit the private M8 administration surface.
            self.assertFalse((SOURCE / "admin").exists())

    def test_dependency_direction_stays_inside_runtime_layers(self) -> None:
        graph = _module_graph()
        for module, dependencies in graph.items():
            if module.startswith("contracts."):
                self.assertFalse(
                    any(dep.startswith(("coordinator", "evolution", "remote", "adapters"))
                        for dep in dependencies),
                    f"low-level contract {module} imports an implementation layer",
                )
            if module.startswith("coordinator."):
                self.assertFalse(
                    any(dep.startswith("evolution") for dep in dependencies),
                    f"coordination implementation {module} imports evolution implementation",
                )
            self.assertFalse(
                any(dep.startswith(("scripts", "evidence", "docs")) for dep in dependencies),
                f"product module {module} imports private or historical content",
            )

    def test_claim_transition_and_reconciliation_are_separate_services(self) -> None:
        graph = _module_graph()
        self.assertIn("coordinator.claim", graph["coordinator.service"])
        self.assertIn("coordinator.transitions", graph["coordinator.service"])
        self.assertIn("coordinator.reconciliation", graph["coordinator.service"])
        service = (SOURCE / "coordinator/service.py").read_text(encoding="utf-8")
        self.assertIn("self._task_claims.claim(", service)
        self.assertIn("self._task_transitions.transition(", service)
        self.assertIn("self._reconciliation.reconcile_runtime(", service)

    def test_claim_structural_filters_use_typed_columns(self) -> None:
        claim = (SOURCE / "coordinator/claim.py").read_text(encoding="utf-8")
        migration = (ROOT / "migrations/runtime/0016_structural_task_fields.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("pv.plan_version_id=t.plan_version_id", claim)
        self.assertIn("active.plan_version_id=pv.plan_version_id", claim)
        self.assertIn("t.max_attempts", claim)
        self.assertNotIn("metadata->>'plan_version_id'", claim)
        self.assertNotIn("budget->>'max_attempts'", claim)
        self.assertIn("tasks_plan_version_status_idx", migration)

    def test_legacy_transport_configuration_is_confined_to_compatibility_map(self) -> None:
        config = (SOURCE / "remote/config.py").read_text(encoding="utf-8")
        for path in (SOURCE / "remote").glob("*.py"):
            if path.name == "config.py":
                continue
            text = path.read_text(encoding="utf-8")
            self.assertNotRegex(text, r"\bM[56]_[A-Z0-9_]+\b", str(path))
        self.assertIn('"AGENTIC_RUNTIME_CONTROL_ENDPOINT": "M5_CONTROL_ENDPOINT"', config)


if __name__ == "__main__":
    unittest.main()
