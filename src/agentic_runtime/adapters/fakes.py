from __future__ import annotations

import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from agentic_runtime.contracts.execution import (AdapterCapabilities, ExecutionRequest,
    ExecutionResult, ExecutionStatus, ExecutorClass, ModelRequest, ModelResponse, Usage)
from agentic_runtime.artifacts.store import ArtifactStore


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FakeHarnessAdapter:
    def __init__(self, backend: str = "fake-harness", capabilities: Sequence[str] = ("text",),
                 artifact_store: ArtifactStore | None = None) -> None:
        self.backend = backend
        self.calls = 0
        self._temp_store = tempfile.TemporaryDirectory(prefix="fake-artifacts-") if artifact_store is None else None
        self.store = artifact_store or ArtifactStore(Path(self._temp_store.name))
        self._capabilities = AdapterCapabilities(backend, "1", ExecutorClass.NATIVE_AGENT_PROCESS,
            frozenset(capabilities), frozenset({"health", "start", "status", "interrupt", "terminate", "collect_result"}),
            supports_interrupt=True)
        self._results: dict[str, ExecutionResult] = {}

    def health(self) -> Mapping[str, Any]:
        return {"healthy": True, "backend": self.backend}

    def capabilities(self) -> AdapterCapabilities:
        return self._capabilities

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self.calls += 1
        output = str(request.metadata.get("fake_output", "verified fake result")).encode()
        artifact = self.store.put(output, kind="fake_output", producer_execution_id=request.execution_id,
                                  producer_attempt_id=request.attempt_id, metadata={"fake": True})
        result = ExecutionResult(request.execution_id, request.task_id, request.attempt_id,
            self.backend, request.requested_model, request.requested_model, _now(), _now(),
            ExecutionStatus.SUCCEEDED, 0, (artifact.artifact_id,), hashlib.sha256(output).hexdigest(), Usage(), None, None,
            {"fake": True})
        self._results[request.execution_id] = result
        return result

    def status(self, execution_id: str) -> Mapping[str, Any]:
        return {"execution_id": execution_id, "state": "SUCCEEDED" if execution_id in self._results else "UNKNOWN",
                "process_alive": False, "result_committed": execution_id in self._results}

    def interrupt(self, execution_id: str) -> bool:
        return execution_id in self._results

    def terminate(self, execution_id: str) -> bool:
        return execution_id in self._results

    def collect_result(self, execution_id: str) -> ExecutionResult:
        return self._results[execution_id]


class FakeModelAdapter:
    def __init__(self, model: str = "fake-model") -> None:
        self.model = model

    def health(self) -> Mapping[str, Any]:
        return {"healthy": True}

    def capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities("fake-model-adapter", "1", ExecutorClass.REMOTE_API_CALL,
            frozenset({"structured_output", "text_generation"}), frozenset({"complete"}),
            (self.model,), "API")

    def complete(self, request: ModelRequest) -> ModelResponse:
        content = str(request.metadata.get("response", "{}"))
        return ModelResponse(request.request_id, "fake-model-adapter", request.requested_model,
                             self.model, content)


class FakeDecisionAdapter:
    def decide(self, policy_id: str, candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        if not candidates:
            return {"selected": None, "policy_id": policy_id, "reason": "no candidates"}
        return {"selected": candidates[0].get("id"), "policy_id": policy_id,
                "reason": "deterministic-first"}
