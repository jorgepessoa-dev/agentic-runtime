from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.contracts.execution import (AdapterCapabilities, ExecutionRequest,
    ExecutionResult, ExecutionStatus, ExecutorClass, Usage)
from agentic_runtime.execution.process_supervisor import ProcessSupervisor
from agentic_runtime.telemetry.execution_log import ExecutionLog


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class CommandHarnessAdapter:
    """Generic native CLI adapter. Product-specific argv construction is injected."""

    def __init__(self, backend: str, version: str, command_factory: Callable[[ExecutionRequest], Sequence[str]],
                 capabilities: Sequence[str], artifact_store: ArtifactStore,
                 supervisor: ProcessSupervisor | None = None, adapter_version: str = "0.1",
                 execution_log: ExecutionLog | None = None,
                 output_normalizer: Callable[[bytes], bytes] | None = None) -> None:
        self.backend, self.version, self.command_factory = backend, version, command_factory
        self.store = artifact_store
        self.supervisor = supervisor or ProcessSupervisor()
        self.execution_log = execution_log
        self.output_normalizer = output_normalizer
        self._caps = AdapterCapabilities(backend, adapter_version, ExecutorClass.NATIVE_AGENT_PROCESS,
            frozenset(capabilities), frozenset({"health", "start", "status", "interrupt", "terminate", "collect_result"}),
            supports_interrupt=True)
        self._started_at: dict[str, str] = {}
        self._requests: dict[str, ExecutionRequest] = {}
        self._collected: dict[str, ExecutionResult] = {}

    def health(self) -> Mapping[str, Any]:
        available = shutil.which(self.backend) is not None
        return {"executable_found": available, "backend": self.backend, "version": self.version,
                "provider_health": "UNKNOWN", "authentication": "NOT_CHECKED"}

    def capabilities(self) -> AdapterCapabilities:
        return self._caps

    def start(self, request: ExecutionRequest) -> Mapping[str, Any]:
        if request.capability not in self._caps.capabilities:
            raise PermissionError(f"backend lacks capability {request.capability!r}")
        workspace = Path(request.workspace_ref).resolve(strict=True)
        command = self.command_factory(request)
        secret_markers = ("API_KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")
        env = {k: v for k, v in os.environ.items()
               if not any(marker in k.upper() for marker in secret_markers)}
        managed = self.supervisor.start(request.execution_id, command, workspace, env, request.timeout_seconds)
        self._started_at[request.execution_id] = _now()
        self._requests[request.execution_id] = request
        return {"execution_id": request.execution_id, "pid": managed.process.pid,
                "started": True, "process_alive": True, "last_heartbeat": self._started_at[request.execution_id]}

    def status(self, execution_id: str) -> Mapping[str, Any]:
        return self.supervisor.status(execution_id)

    def interrupt(self, execution_id: str) -> bool:
        return self.supervisor.cancel(execution_id).state == "CANCELLED"

    def terminate(self, execution_id: str) -> bool:
        return self.interrupt(execution_id)

    def collect_result(self, execution_id: str) -> ExecutionResult:
        if execution_id in self._collected:
            return self._collected[execution_id]
        managed = self.supervisor.wait(execution_id)
        request = self._requests[execution_id]
        stdout, stderr = self.supervisor.output(execution_id)
        result_file = request.metadata.get("result_file")
        if result_file:
            final_path = Path(request.workspace_ref) / str(result_file)
            if final_path.is_file():
                stdout = final_path.read_bytes()
        if self.output_normalizer:
            stdout = self.output_normalizer(stdout)
        status = {"SUCCEEDED": ExecutionStatus.SUCCEEDED, "TIMED_OUT": ExecutionStatus.TIMED_OUT,
                  "CANCELLED": ExecutionStatus.CANCELLED}.get(managed.state, ExecutionStatus.FAILED)
        artifact_refs: tuple[str, ...] = ()
        output_hash = None
        if status == ExecutionStatus.SUCCEEDED:
            artifact = self.store.put(stdout, kind="execution_stdout", producer_execution_id=execution_id,
                producer_attempt_id=request.attempt_id,
                metadata={"backend": self.backend, "configuration_ref": self.version})
            artifact_refs, output_hash = (artifact.artifact_id,), artifact.content_hash
        error = None if status == ExecutionStatus.SUCCEEDED else f"{managed.reason or 'process failed'}; stderr_bytes={len(stderr)}"
        result = ExecutionResult(execution_id, request.task_id, request.attempt_id, self.backend,
            request.requested_model, None, self._started_at[execution_id], _now(), status,
            managed.exit_code, artifact_refs, output_hash, Usage(), None, error,
            {"duration_seconds": round(time.monotonic() - managed.started_monotonic, 3),
             "peak_rss_kb": managed.peak_rss_kb, "process_alive": False,
             "result_committed": status == ExecutionStatus.SUCCEEDED,
             "adapter_version": self._caps.adapter_version, "configuration_ref": self.version,
             "capability": request.capability, "executor_class": request.executor_class.value,
             "execution_location": request.execution_location.value,
             "resource_class": request.resource_class,
             "retry_count": int(request.metadata.get("retry_count", 0)),
             "artifact_count": len(artifact_refs), "input_tokens": None, "output_tokens": None,
             "estimated_cost": None, "error_class": None if status == ExecutionStatus.SUCCEEDED else status.value})
        if status == ExecutionStatus.SUCCEEDED and (not artifact_refs or self.store.verify(artifact_refs[0]).content_hash != output_hash):
            raise RuntimeError("output artifact verification failed")
        self._collected[execution_id] = result
        managed.result_committed = status == ExecutionStatus.SUCCEEDED
        if self.execution_log:
            self.execution_log.append({"execution_id": execution_id, "task_id": request.task_id,
                "attempt_id": request.attempt_id, "sandbox_id": request.metadata.get("sandbox_id"),
                "backend": self.backend, "harness": request.metadata.get("harness"),
                "requested_model": request.requested_model, "resolved_model": None,
                "capability": request.capability, "resource_class": request.resource_class,
                "execution_location": request.execution_location.value,
                "started_at": result.started_at, "ended_at": result.ended_at,
                "duration_seconds": result.telemetry["duration_seconds"],
                "terminal_status": result.status.value, "exit_code": result.exit_code,
                "retry_count": result.telemetry["retry_count"], "artifact_count": len(result.artifact_refs),
                "input_tokens": None, "output_tokens": None, "cost": None,
                "peak_rss_kb": result.telemetry["peak_rss_kb"],
                "error_class": result.telemetry["error_class"],
                "configuration_ref": self.version})
        return result

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self.start(request)
        return self.collect_result(request.execution_id)

    def close(self, execution_id: str) -> None:
        self.supervisor.forget(execution_id)
