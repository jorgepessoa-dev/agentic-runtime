from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from agentic_runtime.adapters.fakes import FakeDecisionAdapter, FakeHarnessAdapter, FakeModelAdapter
from agentic_runtime.adapters.command_harness import CommandHarnessAdapter
from agentic_runtime.artifacts.store import ArtifactIntegrityError, ArtifactStore
from agentic_runtime.contracts.execution import (AdapterCapabilities, ExecutionMode, ExecutionRequest,
    ExecutionResult, ExecutionStatus, ExecutorClass, ModelRequest)
from agentic_runtime.contracts.subtask import LocalSubtaskAuthorizer, SubtaskRequest
from agentic_runtime.execution.process_supervisor import ProcessSupervisor
from agentic_runtime.execution.registry import ExecutorRegistration, ExecutorRegistry, validate_mode
from agentic_runtime.execution.router import ExecutionRouter, ExecutionInProgress, IdempotencyConflict
from agentic_runtime.adapters.sandbox import GitWorktreeSandboxAdapter


def sample_request(workspace: Path, *, key: str = "logical-1", execution_id: str = "exec-1") -> ExecutionRequest:
    return ExecutionRequest(execution_id, "task-1", "attempt-1", "code_review",
        ExecutorClass.NATIVE_AGENT_PROCESS, "fake", None, str(workspace), output_contract="text",
        execution_mode=ExecutionMode.CHEAP, idempotency_key=key)


class FakeAdapterTests(unittest.TestCase):
    def test_fake_model_and_decision_boundaries(self) -> None:
        model = FakeModelAdapter()
        response = model.complete(ModelRequest("r1", "structured_output", None, (), "produce json",
                                                "json", 1, {"response": '{"ok":true}'}))
        self.assertEqual(json.loads(response.content), {"ok": True})
        decision = FakeDecisionAdapter().decide("first", [{"id": "a"}, {"id": "b"}])
        self.assertEqual(decision["selected"], "a")

    def test_execution_modes_have_explicit_minimum_gates(self) -> None:
        cap_a = AdapterCapabilities("a", "1", ExecutorClass.NATIVE_AGENT_PROCESS,
                                    frozenset({"task"}), frozenset())
        cap_b = AdapterCapabilities("b", "1", ExecutorClass.NATIVE_AGENT_PROCESS,
                                    frozenset({"task"}), frozenset())
        a = ExecutorRegistration("a", "a", cap_a, "a-v1", object())
        b = ExecutorRegistration("b", "b", cap_b, "b-v1", object())
        validate_mode(ExecutionMode.CHEAP, (a,))
        validate_mode(ExecutionMode.DIVERSE, (a, b))
        with self.assertRaises(ValueError):
            validate_mode(ExecutionMode.DIVERSE, (a,))
        validate_mode(ExecutionMode.CRITICAL, (a,), independent_reviewer=True,
                      deterministic_validation=True)
        with self.assertRaises(ValueError):
            validate_mode(ExecutionMode.CRITICAL, (a,), independent_reviewer=True)


class IdempotencyTests(unittest.TestCase):
    def test_duplicate_returns_same_committed_result_without_reexecution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = FakeHarnessAdapter("fake", ("code_review",), ArtifactStore(root / "fake-artifacts"))
            registry = ExecutorRegistry()
            registry.register(ExecutorRegistration("fake-1", "fake", adapter.capabilities(), "fake-config-v1", adapter))
            router = ExecutionRouter(registry, root / "ledger.sqlite", adapter.store.verify)
            request = sample_request(root)
            first = router.execute(request)
            duplicate = router.execute(request)
            self.assertEqual(first, duplicate)
            self.assertEqual(adapter.calls, 1)

    def test_key_reuse_for_different_attempt_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = FakeHarnessAdapter("fake", ("code_review",), ArtifactStore(root / "fake-artifacts"))
            registry = ExecutorRegistry()
            registry.register(ExecutorRegistration("fake-1", "fake", adapter.capabilities(), "v1", adapter))
            router = ExecutionRouter(registry, root / "ledger.sqlite", adapter.store.verify)
            router.execute(sample_request(root))
            changed = ExecutionRequest("exec-2", "task-1", "attempt-2", "code_review",
                ExecutorClass.NATIVE_AGENT_PROCESS, "fake", None, str(root), idempotency_key="logical-1")
            with self.assertRaises(IdempotencyConflict):
                router.execute(changed)

    def test_success_without_independent_artifact_verification_is_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = FakeHarnessAdapter("fake", ("code_review",), ArtifactStore(root / "fake-artifacts"))
            registry = ExecutorRegistry()
            registry.register(ExecutorRegistration("fake-1", "fake", adapter.capabilities(), "v1", adapter))
            router = ExecutionRouter(registry, root / "ledger.sqlite")
            request = sample_request(root)
            with self.assertRaisesRegex(ValueError, "artifact verifier"):
                router.execute(request)
            with self.assertRaises(ExecutionInProgress):
                router.execute(request)


class ArtifactTests(unittest.TestCase):
    def test_content_addressed_atomic_store_and_corruption_detection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ArtifactStore(Path(tmp) / "artifacts")
            manifest = store.put(b"immutable-output", kind="test", producer_execution_id="e1",
                                 producer_attempt_id="a1")
            self.assertEqual(manifest.content_hash, hashlib.sha256(b"immutable-output").hexdigest())
            self.assertEqual(store.read(manifest.artifact_id), b"immutable-output")
            blob = store.path_for(manifest.artifact_id) / "content"
            blob.write_bytes(b"mutated-output")
            with self.assertRaises(ArtifactIntegrityError):
                store.verify(manifest.artifact_id)

    def test_duplicate_content_cannot_overwrite_artifact_producer_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store=ArtifactStore(Path(tmp)/"artifacts")
            payload=b"same bytes, separate attempts"
            original=store.put(payload,kind="result",producer_execution_id="exec-a",producer_attempt_id="attempt-a")
            with self.assertRaises(ArtifactIntegrityError):
                store.put(payload,kind="result",producer_execution_id="exec-b",producer_attempt_id="attempt-b")
            self.assertEqual(store.verify(original.artifact_id).producer_attempt_id,"attempt-a")


def live_pid(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text()
        state = state[state.rfind(")") + 2:].split()[0]
        return state not in {"Z", "X"}
    except (OSError, IndexError):
        return False


class ProcessFaultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.supervisor = ProcessSupervisor(terminate_grace_seconds=0.1, poll_seconds=0.02)
        self.cwd = Path(tempfile.mkdtemp(prefix="m2-proc-test-"))

    def tearDown(self) -> None:
        for execution_id in list(self.supervisor._processes):
            managed = self.supervisor._processes[execution_id]
            if managed.state == "RUNNING":
                self.supervisor.cancel(execution_id)
            self.supervisor.forget(execution_id)
        self.cwd.rmdir()

    def test_command_harness_start_status_interrupt_and_collect(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ArtifactStore(root / "artifacts")
            adapter = CommandHarnessAdapter("bash", "system", lambda req: ["bash", "-c", "sleep 30"],
                ("text",), store, ProcessSupervisor(0.1, 0.02))
            request = ExecutionRequest("adapter-cancel", "task", "attempt", "text",
                ExecutorClass.NATIVE_AGENT_PROCESS, "bash", None, str(root), timeout_seconds=5,
                idempotency_key="adapter-cancel-key")
            started = adapter.start(request)
            self.assertTrue(started["started"])
            self.assertTrue(adapter.status("adapter-cancel")["process_alive"])
            self.assertTrue(adapter.interrupt("adapter-cancel"))
            result = adapter.collect_result("adapter-cancel")
            self.assertEqual(result.status, ExecutionStatus.CANCELLED)
            self.assertFalse(result.artifact_refs)
            self.assertFalse(result.telemetry["process_alive"])
            adapter.close("adapter-cancel")

    def test_timeout_terminates_whole_process_group(self) -> None:
        p = self.supervisor.start("timeout", ["bash", "-c", "sleep 30 & echo $!; wait"], self.cwd,
                                  dict(os.environ), 0.25)
        result = self.supervisor.wait("timeout")
        out, _ = self.supervisor.output("timeout")
        child_pid = int(out.decode().strip().splitlines()[0])
        self.assertEqual(result.state, "TIMED_OUT")
        self.assertFalse(self.supervisor.status("timeout")["result_committed"])
        self.assertFalse(live_pid(child_pid))

    def test_manual_cancellation_terminates_whole_process_group(self) -> None:
        self.supervisor.start("cancel", ["bash", "-c", "sleep 30 & echo $!; wait"], self.cwd,
                              dict(os.environ), 10)
        time.sleep(0.15)
        out, _ = self.supervisor.output("cancel")
        child_pid = int(out.decode().strip().splitlines()[0])
        result = self.supervisor.cancel("cancel")
        self.assertEqual(result.state, "CANCELLED")
        self.assertFalse(self.supervisor.status("cancel")["result_committed"])
        self.assertFalse(live_pid(child_pid))

    def test_worker_crash_reaps_leftover_child_and_never_commits(self) -> None:
        self.supervisor.start("crash", ["bash", "-c", "sleep 30 & echo $!; exit 17"], self.cwd,
                              dict(os.environ), 5)
        result = self.supervisor.wait("crash")
        out, _ = self.supervisor.output("crash")
        child_pid = int(out.decode().strip().splitlines()[0])
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.exit_code, 17)
        self.assertFalse(self.supervisor.status("crash")["result_committed"])
        self.assertFalse(live_pid(child_pid))


def init_repo(path: Path) -> str:
    subprocess.run(["git", "init", str(path)], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "test@example.invalid"], check=True)
    (path / "base.txt").write_text("base\n")
    subprocess.run(["git", "-C", str(path), "add", "base.txt"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-m", "base"], check=True,
                   stdout=subprocess.DEVNULL)
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


class SandboxTests(unittest.TestCase):
    def test_each_attempt_gets_independent_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"; repo.mkdir()
            revision = init_repo(repo)
            adapter = GitWorktreeSandboxAdapter(repo, root / "sandboxes")
            a = adapter.prepare(adapter.create("task", "a", revision), {"network": "none"})
            b = adapter.prepare(adapter.create("task", "b", revision), {"network": "none"})
            (a.workspace_path / "from-a.txt").write_text("A")
            (b.workspace_path / "from-b.txt").write_text("B")
            self.assertFalse((a.workspace_path / "from-b.txt").exists())
            self.assertFalse((b.workspace_path / "from-a.txt").exists())
            self.assertEqual(a.base_revision, b.base_revision)
            self.assertTrue(adapter.collect_artifacts(a))
            adapter.cleanup(a); adapter.destroy(a)
            adapter.cleanup(b); adapter.destroy(b)
            self.assertFalse(a.workspace_path.exists())


class DelegationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = LocalSubtaskAuthorizer(max_depth=1, max_children_per_task=2, max_total_children=6)
        self.policy.register_parent("root", 2.0)

    def req(self, depth: int = 0, budget: float = 0.5) -> SubtaskRequest:
        return SubtaskRequest("root", "attempt-root", depth, "inspect a neutral text artifact",
                              ("text_analysis",), (), budget)

    def test_subtask_approved_and_budget_conserved(self) -> None:
        result = self.policy.request_subtask(self.req())
        self.assertTrue(result.approved)
        self.assertEqual(result.reserved_budget, 0.5)
        self.assertEqual(self.policy._parents["root"].remaining_budget, 1.5)

    def test_depth_child_count_and_parent_budget_are_enforced(self) -> None:
        self.assertFalse(self.policy.request_subtask(self.req(depth=1)).approved)
        self.policy.request_subtask(self.req())
        self.policy.request_subtask(self.req())
        self.assertFalse(self.policy.request_subtask(self.req()).approved)
        self.assertFalse(self.policy.request_subtask(self.req(budget=3)).approved)


if __name__ == "__main__":
    unittest.main()
